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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
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

    /** Default cadence for the sealed-segment GC tick on the leader. */
    static final Duration DEFAULT_GC_INTERVAL = Duration.ofSeconds(60);

    @Getter
    private final TopicName topicName;
    private final ScalableTopicResources resources;
    private final BrokerService brokerService;
    private final LeaderElection<String> leaderElection;
    /** Wall-clock source used for sealed-segment retention math. Tests override. */
    private final Clock clock;
    /** Cadence of the GC tick. Tests override to a small value. */
    private final Duration gcInterval;

    private volatile SegmentLayout currentLayout;

    /** Per-subscription consumer tracking. */
    private final ConcurrentHashMap<String, SubscriptionCoordinator> subscriptions = new ConcurrentHashMap<>();

    /** Sealed-segment GC scheduled task. Non-null only while this broker is leader. */
    private volatile ScheduledFuture<?> gcTask;

    /** Periodic auto split/merge evaluation task (PIP-483). Non-null only while leader. */
    private volatile ScheduledFuture<?> autoScaleTask;

    /**
     * Serializes auto split/merge: an evaluation acquires this before deciding and holds it
     * for the whole split/merge it dispatches, so concurrent ticks / consumer-change triggers
     * never launch overlapping auto operations.
     */
    private final AtomicBoolean autoScaleInFlight = new AtomicBoolean(false);

    /**
     * Set when a trigger arrives while an evaluation is in flight; the in-flight run
     * re-evaluates once on completion so coalesced triggers are not lost until the next tick.
     */
    private final AtomicBoolean autoScaleReEvaluate = new AtomicBoolean(false);

    /** Epoch millis of the last split on this topic (manual or auto); MIN_VALUE if none. */
    private volatile long lastSplitAtMs = Long.MIN_VALUE;
    /** Epoch millis of the last merge on this topic (manual or auto); MIN_VALUE if none. */
    private volatile long lastMergeAtMs = Long.MIN_VALUE;

    @Getter
    private volatile LeaderElectionState leaderState = LeaderElectionState.NoLeader;

    private volatile boolean closed = false;

    ScalableTopicController(TopicName topicName,
                            ScalableTopicResources resources,
                            BrokerService brokerService,
                            CoordinationService coordinationService) {
        this(topicName, resources, brokerService, coordinationService,
                Clock.systemUTC(), DEFAULT_GC_INTERVAL);
    }

    /**
     * Test constructor: overrides the wall-clock source and the GC tick cadence.
     */
    ScalableTopicController(TopicName topicName,
                            ScalableTopicResources resources,
                            BrokerService brokerService,
                            CoordinationService coordinationService,
                            Clock clock,
                            Duration gcInterval) {
        this.topicName = topicName;
        this.resources = resources;
        this.brokerService = brokerService;
        this.clock = clock;
        this.gcInterval = gcInterval;
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
        if (state != LeaderElectionState.Leading) {
            // Stepped down (or never was leader). Stop the GC and auto-scale ticks so the
            // deposed leader doesn't race the new one on layout writes / backing-topic
            // deletes. The new leader's initialize() will reschedule.
            cancelGcTask();
            cancelAutoScaleTask();
        }
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
                        seedAutoScaleCooldownsFromLayout();
                        scheduleGcTask();
                        scheduleAutoScaleTask();
                        return ensureActiveSegmentsExist()
                                .thenCompose(___ -> restoreSessionsFromStore());
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * Recover the auto split/merge cooldown clocks after winning leadership. The timestamps
     * are in-memory only, but the layout itself records when each segment was created — a
     * split's children have exactly one parent, a merge's child has two — so the most recent
     * creation time of each class is exactly when the last split / merge happened. Without
     * this, every leader failover would reset both cooldowns and e.g. allow an auto merge
     * seconds after one just ran on the previous leader.
     */
    private void seedAutoScaleCooldownsFromLayout() {
        long split = Long.MIN_VALUE;
        long merge = Long.MIN_VALUE;
        for (SegmentInfo segment : currentLayout.getAllSegments().values()) {
            int parents = segment.parentIds().size();
            if (parents == 1) {
                split = Math.max(split, segment.createdAtMs());
            } else if (parents >= 2) {
                merge = Math.max(merge, segment.createdAtMs());
            }
        }
        lastSplitAtMs = split;
        lastMergeAtMs = merge;
    }

    /**
     * Recovery path for active segments whose backing topics are missing — e.g.,
     * a {@code createScalableTopic} call that committed metadata but failed to
     * materialize all initial segments before crashing, or a force-delete of an
     * active segment.
     *
     * <p>Idempotent: {@code createSegmentAsync} on an existing segment is a
     * no-op at the broker (it just loads the existing topic).
     *
     * <p>Sealed segments are intentionally NOT healed here — if a sealed segment's
     * backing topic is gone the data is permanently gone (retention applied or
     * an explicit delete), and re-creating an empty topic would mask that. The
     * V5 checkpoint consumer skips sealed segments whose topics return
     * {@code TopicDoesNotExist}.
     */
    private CompletableFuture<Void> ensureActiveSegmentsExist() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (SegmentInfo seg : currentLayout.getActiveSegments().values()) {
            futures.add(createSegmentTopic(seg, List.of())
                    .exceptionally(ex -> {
                        log.warn().attr("segmentId", seg.segmentId())
                                .exceptionMessage(ex)
                                .log("Failed to ensure active segment topic at controller init; "
                                        + "next attempt to use this segment will retry");
                        return null;
                    }));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Schedule the periodic sealed-segment GC tick. Only fires on the controller leader;
     * idempotent (re-entry just no-ops). Cancelled on close / leader-loss.
     */
    private synchronized void scheduleGcTask() {
        if (closed || gcTask != null) {
            return;
        }
        gcTask = scheduler().scheduleAtFixedRate(this::runGcTickSafely,
                gcInterval.toMillis(), gcInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void cancelGcTask() {
        if (gcTask != null) {
            gcTask.cancel(false);
            gcTask = null;
        }
    }

    private ScheduledExecutorService scheduler() {
        return brokerService.getPulsar().getExecutor();
    }

    private void runGcTickSafely() {
        if (!isLeader() || closed) {
            return;
        }
        try {
            runGcTickAsync().exceptionally(ex -> {
                log.warn().exceptionMessage(ex).log("Scalable-topic GC tick failed");
                return null;
            });
        } catch (Throwable t) {
            // Defensive: scheduleAtFixedRate would suppress the next firing if a tick
            // throws synchronously, so log and swallow here.
            log.warn().exception(t).log("Scalable-topic GC tick threw");
        }
    }

    // --- Auto split/merge (PIP-483) ---

    /**
     * Schedule the periodic traffic-driven auto split/merge evaluation. Only fires on the
     * controller leader; idempotent. Cancelled on close / leader-loss. Consumer-count
     * changes are handled event-driven (see {@link #onConsumerCountChanged()}), not by this
     * tick.
     *
     * <p>The tick is scheduled even when auto-scaling is currently disabled: the enabled
     * flag is dynamic and re-checked on every evaluation, so flipping it on takes effect at
     * the next tick rather than waiting for a leadership cycle. A disabled tick is a cheap
     * no-op.
     */
    private synchronized void scheduleAutoScaleTask() {
        if (closed || autoScaleTask != null) {
            return;
        }
        ServiceConfiguration config = brokerConfig();
        if (config == null) {
            return;
        }
        long intervalMs = Duration.ofSeconds(
                config.getScalableTopicAutoScaleIntervalSeconds()).toMillis();
        if (intervalMs <= 0) {
            return;
        }
        autoScaleTask = scheduler().scheduleAtFixedRate(
                () -> runAutoScaleSafely("tick"), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void cancelAutoScaleTask() {
        if (autoScaleTask != null) {
            autoScaleTask.cancel(false);
            autoScaleTask = null;
        }
    }

    /**
     * Event-driven trigger: a stream/checkpoint consumer registered or unregistered, which
     * may change the per-subscription consumer count. Evaluates the consumer-count split rule
     * within seconds rather than waiting for the periodic tick.
     */
    private void onConsumerCountChanged() {
        runAutoScaleSafely("consumer-change");
    }

    private void runAutoScaleSafely(String trigger) {
        if (!isLeader() || closed) {
            return;
        }
        try {
            evaluateAndAct(trigger).exceptionally(ex -> {
                log.warn().attr("trigger", trigger).exceptionMessage(ex)
                        .log("Auto split/merge evaluation failed");
                return null;
            });
        } catch (Throwable t) {
            log.warn().attr("trigger", trigger).exception(t)
                    .log("Auto split/merge evaluation threw");
        }
    }

    /**
     * Collect the current inputs, run the pure {@link AutoScalePolicyEvaluator}, and dispatch
     * the resulting action. At most one auto operation runs at a time: {@link #autoScaleInFlight}
     * is held from before the decision through the end of the dispatched split/merge.
     */
    private CompletableFuture<Void> evaluateAndAct(String trigger) {
        ServiceConfiguration brokerConfig = brokerConfig();
        if (brokerConfig == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (!autoScaleInFlight.compareAndSet(false, true)) {
            // Another evaluation or auto operation is already running. Don't drop the
            // trigger: mark it pending so the in-flight run re-evaluates on completion —
            // e.g. a consumer registering mid-evaluation would otherwise not be considered
            // until the next periodic tick.
            autoScaleReEvaluate.set(true);
            return CompletableFuture.completedFuture(null);
        }
        try {
            return resolveAutoScaleConfig(brokerConfig)
                    .thenCompose(config -> {
                        if (!config.enabled()) {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        return collectConsumerCounts()
                                .thenCombine(collectLoadSamples(), (consumers, load) ->
                                        AutoScalePolicyEvaluator.decide(currentLayout, load,
                                                consumers, config, clock.millis(),
                                                lastSplitAtMs, lastMergeAtMs))
                                .thenCompose(decision -> dispatch(decision, config, trigger));
                    })
                    .whenComplete((__, ex) -> {
                        autoScaleInFlight.set(false);
                        if (autoScaleReEvaluate.getAndSet(false)) {
                            // Re-run off the completion thread for the trigger(s) coalesced
                            // while this evaluation was in flight.
                            scheduler().execute(() -> runAutoScaleSafely("coalesced"));
                        }
                    });
        } catch (Throwable t) {
            // A synchronous throw between the CAS and the future chain would otherwise leave
            // the in-flight flag set forever, silently disabling auto-scaling on this topic.
            autoScaleInFlight.set(false);
            throw t;
        }
    }

    /**
     * Resolve the effective auto split/merge policy for this topic: broker defaults overlaid
     * with the namespace-level override ({@code Policies.scalableTopicAutoScalePolicy}) and
     * then the per-topic override ({@code ScalableTopicMetadata.autoScalePolicy}). Both reads
     * are metadata-cache-backed, so this is cheap per evaluation and override changes take
     * effect on the next tick without controller restarts.
     *
     * <p>Set-time validation is best-effort only (the namespace override can change after a
     * topic override was validated against it, and broker defaults can change across
     * restarts), so the stored combination can be invalid here. In that case auto split/merge
     * is treated as <b>disabled</b> for the topic — predictable, and loudly logged on every
     * evaluation until an operator fixes the overrides — rather than failing the evaluation
     * chain.
     */
    private CompletableFuture<AutoScaleConfig> resolveAutoScaleConfig(
            ServiceConfiguration brokerConfig) {
        CompletableFuture<AutoScalePolicyOverride> namespaceOverride =
                brokerService.getPulsar().getPulsarResources().getNamespaceResources()
                        .getPoliciesAsync(topicName.getNamespaceObject())
                        .thenApply(opt -> opt.map(p -> p.scalableTopicAutoScalePolicy)
                                .orElse(null));
        CompletableFuture<AutoScalePolicyOverride> topicOverride =
                resources.getScalableTopicMetadataAsync(topicName)
                        .thenApply(opt -> opt.map(ScalableTopicMetadata::getAutoScalePolicy)
                                .orElse(null));
        return namespaceOverride.thenCombine(topicOverride, (ns, topic) -> {
            try {
                return AutoScaleConfig.resolve(brokerConfig, ns, topic);
            } catch (IllegalArgumentException e) {
                log.warn().attr("reason", e.getMessage())
                        .log("Resolved auto split/merge policy is invalid; treating auto "
                                + "split/merge as disabled for this topic until the namespace "
                                + "or topic override is fixed");
                return AutoScaleConfig.fromBrokerConfig(brokerConfig)
                        .toBuilder().enabled(false).build();
            }
        });
    }

    private CompletableFuture<Void> dispatch(AutoScaleDecision decision, AutoScaleConfig config,
                                             String trigger) {
        if (decision instanceof AutoScaleDecision.Split split) {
            log.info().attr("segmentId", split.segmentId()).attr("reason", split.reason())
                    .attr("trigger", trigger).log("Auto split");
            return splitSegment(split.segmentId())
                    .thenApply(__ -> {
                        scheduleFollowUpEvaluation(config);
                        return null;
                    });
        }
        if (decision instanceof AutoScaleDecision.Merge merge) {
            log.info().attr("segmentId1", merge.segmentId1()).attr("segmentId2", merge.segmentId2())
                    .attr("reason", merge.reason()).attr("trigger", trigger).log("Auto merge");
            return mergeSegments(merge.segmentId1(), merge.segmentId2()).thenApply(__ -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * After a successful auto split, schedule one follow-up evaluation right after the split
     * cooldown expires. A burst of consumers joining at once needs one split per cooldown to
     * converge (e.g. 1 segment → N); without this it converges one split per periodic tick
     * instead, which is slower whenever the cooldown is shorter than the tick. The chain
     * stops naturally at the first evaluation that decides {@code NoAction}.
     */
    private void scheduleFollowUpEvaluation(AutoScaleConfig config) {
        if (closed || !isLeader()) {
            return;
        }
        long delayMs = config.splitCooldown().toMillis() + 1;
        scheduler().schedule(() -> runAutoScaleSafely("post-split"),
                delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Per-subscription consumer counts for the controller-managed (STREAM/CHECKPOINT)
     * subscriptions. QUEUE subscriptions bypass the controller and have no coordinator here,
     * so they are naturally excluded — exactly the set the consumer-count split rule wants.
     */
    private CompletableFuture<Map<String, Integer>> collectConsumerCounts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        subscriptions.forEach((name, coordinator) ->
                counts.put(name, coordinator.getConsumers().size()));
        return CompletableFuture.completedFuture(counts);
    }

    /** Read the load record (value + Stat modified time) for every active segment. */
    private CompletableFuture<Map<Long, SegmentLoadSample>> collectLoadSamples() {
        Map<Long, SegmentLoadSample> samples = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (Long segmentId : currentLayout.getActiveSegments().keySet()) {
            futures.add(resources.getSegmentLoadAsync(topicName, segmentId)
                    .thenAccept(opt -> opt.ifPresent(result -> samples.put(segmentId,
                            new SegmentLoadSample(result.getValue(),
                                    result.getStat().getModificationTimestamp())))));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(__ -> samples);
    }

    private ServiceConfiguration brokerConfig() {
        return brokerService.getPulsar().getConfig();
    }

    /**
     * Run one auto split/merge evaluation synchronously-awaitable, for tests. Production code
     * triggers evaluation via the periodic tick and consumer-change events.
     */
    @VisibleForTesting
    CompletableFuture<Void> evaluateAutoScaleForTest() {
        return evaluateAndAct("test");
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

    /**
     * Restore-path entry: consumer type isn't persisted in metadata yet, so we don't
     * know whether the original subscription was STREAM (needs parent-drain ordering)
     * or CHECKPOINT / QUEUE (mustn't have it — CHECKPOINT never drains parents because
     * it doesn't create per-segment cursors). Default to <em>no enforcement</em>; on the
     * first register-after-restore the controller calls
     * {@link SubscriptionCoordinator#installDrainChecker} if the type is STREAM.
     */
    private SubscriptionCoordinator createCoordinator(String subscription) {
        return createCoordinator(subscription, null);
    }

    private SubscriptionCoordinator createCoordinator(String subscription,
            ScalableConsumerType consumerType) {
        // Parent-drain ordering matters only for STREAM consumers (Exclusive per-segment
        // subscription with broker-tracked cursors → preserving per-key order across a
        // split requires waiting for the parent to drain before handing out children).
        // CHECKPOINT consumers track position client-side via Checkpoints and don't even
        // create per-segment cursors — their parent never reports as drained, so the
        // ordering machinery would block their children indefinitely. QUEUE consumers
        // are shared and accept out-of-order delivery by design. Null type (restore
        // path) starts without a checker; it's installed lazily on first STREAM
        // register.
        SegmentDrainChecker checker =
                consumerType == ScalableConsumerType.STREAM ? this::isSegmentDrained : null;

        // Defensive: PulsarService.getConfig() is null in some unit-test mocks. Fall
        // back to the SubscriptionCoordinator's default grace period in that case.
        var config = brokerService.getPulsar().getConfig();
        if (config == null) {
            return new SubscriptionCoordinator(
                    subscription,
                    topicName,
                    currentLayout,
                    resources,
                    brokerService.getPulsar().getExecutor());
        }
        Duration gracePeriod = Duration.ofSeconds(
                config.getScalableTopicConsumerSessionGracePeriodSeconds());
        return new SubscriptionCoordinator(
                subscription,
                topicName,
                currentLayout,
                resources,
                brokerService.getPulsar().getExecutor(),
                gracePeriod,
                checker,
                SubscriptionCoordinator.DEFAULT_DRAIN_INITIAL_DELAY,
                SubscriptionCoordinator.DEFAULT_DRAIN_MAX_DELAY);
    }

    /**
     * Drain check used by every {@link SubscriptionCoordinator} on this topic. Asks the
     * segment topic's owning broker for the per-subscription backlog via the
     * {@code /segments/.../subscription/.../backlog} admin endpoint, which redirects to
     * the topic owner — works whether the controller and the segment colocate or not.
     *
     * <p>Returns {@code false} if the segment topic or subscription is not yet loaded
     * (the admin endpoint replies 404). The next poll will succeed once the consumer's
     * subscribe lands the topic on its owning broker.
     */
    private CompletableFuture<Boolean> isSegmentDrained(SegmentInfo segment, String subscription) {
        String segmentTopicName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics()
                    .getSegmentSubscriptionBacklogAsync(segmentTopicName, subscription)
                    .thenApply(backlog -> backlog != null && backlog <= 0)
                    .exceptionally(ex -> {
                        Throwable cause =
                                org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException) {
                            // Topic or subscription not loaded yet — try again on the
                            // next poll. The consumer's subscribe will materialize it.
                            return false;
                        }
                        throw org.apache.pulsar.common.util.FutureUtil.wrapToCompletionException(cause);
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
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

        // Single timestamp shared by the local preview and the CAS-retried metadata update,
        // so the children's createdAtMs and the parent's sealedAtMs always agree even if the
        // CAS retries due to concurrent writers.
        final long nowMs = clock.millis();

        // Compute the new layout locally to derive child segment info
        SegmentLayout newLayout = currentLayout.splitSegment(segmentId, nowMs);
        SegmentInfo child1 = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 2);
        SegmentInfo child2 = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 1);
        SegmentInfo parent = currentLayout.getAllSegments().get(segmentId);
        String parentTopicName = toSegmentPersistentName(parent);

        // Step 1: Read the scalable topic's subscriptions from metadata (the single source
        // of truth — segment topics may live on different brokers, but the subscription set
        // is tracked here), then create child segment topics with those subscriptions
        // already provisioned (the create call routes to each segment's owning broker).
        return resources.listSubscriptionsAsync(topicName)
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
              SegmentLayout updated = latest.splitSegment(segmentId, nowMs);
              return updated.toMetadata(md);
          }))
          .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topicName, true))
          .thenCompose(optMd -> {
              currentLayout = SegmentLayout.fromMetadata(optMd.orElseThrow());
              // Start the auto-split cooldown only now that the split actually happened
              // (covers manual and auto splits; a failed attempt doesn't burn the cooldown).
              lastSplitAtMs = nowMs;

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

        // Single timestamp shared by the local preview and the CAS-retried metadata
        // update — see splitSegment for the rationale.
        final long nowMs = clock.millis();

        // Compute the new layout locally to derive merged segment info
        SegmentLayout newLayout = currentLayout.mergeSegments(segmentId1, segmentId2, nowMs);
        SegmentInfo merged = newLayout.getAllSegments().get(newLayout.getNextSegmentId() - 1);
        SegmentInfo parent1 = currentLayout.getAllSegments().get(segmentId1);
        SegmentInfo parent2 = currentLayout.getAllSegments().get(segmentId2);
        String parent1Topic = toSegmentPersistentName(parent1);
        String parent2Topic = toSegmentPersistentName(parent2);

        // Step 1: Read the scalable topic's subscriptions from metadata (single source of
        // truth, see splitSegment), then create the merged segment topic with those
        // subscriptions provisioned.
        return resources.listSubscriptionsAsync(topicName)
          .thenCompose(parentSubs -> createSegmentTopic(merged, new java.util.ArrayList<>(parentSubs)))

          // Step 2: Terminate both parent segment topics
          .thenCompose(__ -> terminateSegmentTopic(parent1Topic))
          .thenCompose(__ -> terminateSegmentTopic(parent2Topic))

          // Step 3: Atomic metadata update (only after topic + cursors are ready + parents terminated)
          .thenCompose(__ -> resources.updateScalableTopicAsync(topicName, md -> {
              SegmentLayout latest = SegmentLayout.fromMetadata(md);
              SegmentLayout updated = latest.mergeSegments(segmentId1, segmentId2, nowMs);
              return updated.toMetadata(md);
          }))
          .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topicName, true))
          .thenCompose(optMd -> {
              currentLayout = SegmentLayout.fromMetadata(optMd.orElseThrow());
              // Start the auto-merge cooldown only now that the merge actually happened
              // (covers manual and auto merges; a failed attempt doesn't burn the cooldown).
              lastMergeAtMs = nowMs;
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
     *
     * <p>The {@code consumerType} is used at coordinator creation time to decide whether
     * to enforce parent-drain ordering on assignments — see
     * {@link SubscriptionCoordinator}. The coordinator's setting is fixed at first
     * registration (a subscription's type doesn't change in practice); subsequent
     * registers with a different type still work but won't change the ordering policy.
     */
    public CompletableFuture<ConsumerAssignment> registerConsumer(String subscription,
                                                                   String consumerName,
                                                                   long consumerId,
                                                                   ScalableConsumerType
                                                                           consumerType,
                                                                   TransportCnx cnx) {
        checkLeader();
        SubscriptionCoordinator coordinator = subscriptions.computeIfAbsent(
                subscription, sub -> createCoordinator(sub, consumerType));
        // The coordinator may have been created on the failover-restore path (consumer
        // type unknown then; we defaulted to "no parent-drain enforcement"). Now that we
        // know the type, upgrade if it's STREAM. installDrainChecker is a no-op if the
        // coordinator already has a checker, so safe to call unconditionally.
        if (consumerType == ScalableConsumerType.STREAM) {
            coordinator.installDrainChecker(this::isSegmentDrained);
        }
        return coordinator.registerConsumer(consumerName, consumerId, cnx)
                .thenApply(assignments -> {
                    // A new consumer may now outnumber the segments — evaluate the
                    // consumer-count split rule promptly rather than waiting for the tick.
                    onConsumerCountChanged();
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

    /**
     * Reset a subscription's cursor across every segment to the given wall-clock
     * timestamp. We use each segment's recorded {@code [createdAtMs, sealedAtMs)}
     * window to dispatch the cheapest possible per-segment op:
     *
     * <ul>
     *   <li>Segment was sealed before {@code timestampMs} — all of its data is from
     *       earlier; cursor → end of segment (skip-all).</li>
     *   <li>Segment was created at-or-after {@code timestampMs} — all of its data is
     *       from at-or-after; cursor → earliest (seek to {@code timestamp=0}).</li>
     *   <li>Segment is alive at {@code timestampMs} (active or straddling sealed) —
     *       cursor seeks to {@code timestamp}.</li>
     * </ul>
     *
     * <p>Per-segment failures are surfaced (the call fails-fast). The only tolerated
     * outcome is {@code 404 Not Found} from the segment endpoint, which the segment
     * REST resource emits exclusively for "subscription not present on this segment"
     * (e.g. the cursor hasn't been materialised yet — it will propagate lazily and
     * the next seek will land it). Transient unloads / ownership churn surface as
     * {@code 503} from the segment endpoint and propagate to the caller, who can
     * retry the parent-level operation.
     */
    public CompletableFuture<Void> seekSubscription(String subscription, long timestampMs) {
        checkLeader();
        SegmentLayout layout = this.currentLayout;
        CompletableFuture<?>[] futures = layout.getAllSegments().values().stream()
                .map(segment -> seekSubscriptionOnSegment(segment, subscription, timestampMs))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    /**
     * Skip every undelivered message on the subscription, across every segment in the
     * DAG. Equivalent to advancing each per-segment cursor to the end.
     */
    public CompletableFuture<Void> clearBacklog(String subscription) {
        checkLeader();
        SegmentLayout layout = this.currentLayout;
        CompletableFuture<?>[] futures = layout.getAllSegments().values().stream()
                .map(segment -> clearSubscriptionBacklogOnSegment(segment, subscription))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> seekSubscriptionOnSegment(SegmentInfo segment,
                                                              String subscription,
                                                              long timestampMs) {
        // Classify the segment relative to the requested timestamp using the recorded
        // sealed-time / created-time. This is what makes the parent-level seek O(N segments)
        // worth of cheap RPCs rather than O(N) timestamp-based scans of every segment's
        // managed ledger.
        if (segment.isSealed() && segment.sealedAtMs() > 0
                && segment.sealedAtMs() <= timestampMs) {
            // Segment fully predates timestamp → skip everything on this segment.
            return clearSubscriptionBacklogOnSegment(segment, subscription);
        }
        long effective = timestampMs;
        if (segment.createdAtMs() > 0 && segment.createdAtMs() >= timestampMs) {
            // Segment fully postdates timestamp → seek to start (timestamp=0 == earliest
            // for managed-ledger reset-cursor-by-timestamp semantics).
            effective = 0L;
        }
        String segmentName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().seekSegmentSubscriptionAsync(segmentName, subscription, effective)
                    .exceptionally(ex -> {
                        Throwable cause =
                                org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException
                                .NotFoundException) {
                            // 404 from the segment endpoint == "subscription not present
                            // on this segment" (the segment endpoint uses 503 for
                            // "topic not loaded"). The cursor will propagate lazily;
                            // tolerated.
                            return null;
                        }
                        throw org.apache.pulsar.common.util.FutureUtil.wrapToCompletionException(cause);
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> clearSubscriptionBacklogOnSegment(SegmentInfo segment,
                                                                       String subscription) {
        String segmentName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics()
                    .clearSegmentSubscriptionBacklogAsync(segmentName, subscription)
                    .exceptionally(ex -> {
                        Throwable cause =
                                org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException
                                .NotFoundException) {
                            // Subscription not present on this segment — tolerated.
                            // (See seek path for the 404-vs-503 contract.)
                            return null;
                        }
                        throw org.apache.pulsar.common.util.FutureUtil.wrapToCompletionException(cause);
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Void> createSubscriptionOnSegment(SegmentInfo segment, String subscription) {
        String segmentTopicName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics()
                    .createSegmentSubscriptionAsync(segmentTopicName, subscription)
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
        String segmentTopicName = toSegmentPersistentName(segment);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics()
                    .deleteSegmentSubscriptionAsync(segmentTopicName, subscription)
                    .exceptionally(ex -> {
                        Throwable cause = org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException) {
                            return null;
                        }
                        log.warn().attr("subscription", subscription)
                                .attr("segment", segmentTopicName).exceptionMessage(cause)
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

    // --- Sealed-segment GC ---

    /**
     * One iteration of the sealed-segment GC. For every sealed segment in the current
     * layout whose retention window has expired, polls every known subscription's
     * backlog on that segment; if all subscriptions are drained, prunes the segment
     * from the DAG (CAS) and deletes its backing managed-ledger topic.
     *
     * <p>The retention window is resolved from topic-policies on the parent
     * {@code topic://...} → namespace policy → broker default, the same precedence
     * Pulsar uses for regular topics.
     *
     * <p>Visible for tests; in production it's invoked by the scheduled task.
     */
    CompletableFuture<Void> runGcTickAsync() {
        if (!isLeader() || closed) {
            return CompletableFuture.completedFuture(null);
        }
        final SegmentLayout layout = currentLayout;
        if (layout == null) {
            return CompletableFuture.completedFuture(null);
        }

        // Candidates: sealed segments past their retention horizon. We resolve
        // retention once per tick — cheap, and avoids per-segment policy lookups.
        return resolveRetentionMillisAsync()
                .thenCompose(retentionMs -> {
                    if (retentionMs == null) {
                        // Negative / unset → retain forever. No GC this tick.
                        return CompletableFuture.completedFuture(null);
                    }
                    long now = clock.millis();
                    List<SegmentInfo> candidates = new ArrayList<>();
                    for (SegmentInfo seg : layout.getAllSegments().values()) {
                        if (seg.isSealed() && seg.sealedAtMs() > 0
                                && (now - seg.sealedAtMs()) >= retentionMs) {
                            candidates.add(seg);
                        }
                    }
                    if (candidates.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return pruneEligibleAsync(candidates);
                });
    }

    /**
     * For each candidate sealed segment, check that every existing subscription has
     * drained it (backlog == 0); prune the ones that pass. The drain checks fan out
     * concurrently, but the resulting layout mutation is coalesced into a <em>single</em>
     * CAS write so multiple eligible segments don't compete on the same metadata znode.
     *
     * <p><b>Subscription-type behaviour.</b> The drain check is the per-segment backlog
     * admin endpoint — Pulsar's standard cursor-position view, which works the same way
     * for STREAM (Exclusive) and QUEUE (Shared) subscriptions: a sealed segment with
     * cursor at the end reports backlog 0. For CHECKPOINT subscriptions there is no
     * broker-side cursor, the endpoint returns {@code NotFoundException}, and
     * {@code isSegmentDrained} reports {@code false} — the segment is treated as
     * "still in use" and never pruned while a CHECKPOINT subscription is registered.
     *
     * <p><b>Parent-vs-child ordering.</b> Sealed segments form a DAG; pruning is allowed
     * in any order because the active leaves always cover the full hash range, and the
     * managed-ledger storage of each segment is independent. {@link SegmentLayout#pruneSegment}
     * rewrites the parent/child edges, so consumers using the post-prune layout see the
     * pruned segment as "no longer present" — equivalent to "drained" for parent-drain
     * ordering.
     */
    private CompletableFuture<Void> pruneEligibleAsync(List<SegmentInfo> candidates) {
        return resources.listSubscriptionsAsync(topicName)
                .thenCompose(subs -> {
                    // Fan out drain checks; collect the survivors.
                    List<CompletableFuture<SegmentInfo>> filtered = new ArrayList<>();
                    for (SegmentInfo seg : candidates) {
                        filtered.add(prunable(seg, subs)
                                .thenApply(ok -> ok ? seg : null)
                                .exceptionally(ex -> {
                                    log.warn().attr("segmentId", seg.segmentId())
                                            .exceptionMessage(ex)
                                            .log("GC: failed to evaluate prunability;"
                                                    + " will retry on next tick");
                                    return null;
                                }));
                    }
                    return CompletableFuture.allOf(filtered.toArray(CompletableFuture[]::new))
                            .thenApply(__ -> {
                                List<SegmentInfo> drained = new ArrayList<>();
                                for (var f : filtered) {
                                    SegmentInfo s = f.join();
                                    if (s != null) {
                                        drained.add(s);
                                    }
                                }
                                return drained;
                            });
                })
                .thenCompose(this::pruneAllAsync);
    }

    /**
     * Coalesce all drained-and-eligible segments into a single layout-mutation CAS,
     * then fan out the per-segment backing-topic deletes. This is the path that
     * actually mutates state. Re-validates leadership before the CAS — drain checks
     * can take seconds, leadership may have flipped in the meantime, and we don't
     * want a deposed leader writing layout updates.
     */
    private CompletableFuture<Void> pruneAllAsync(List<SegmentInfo> drained) {
        if (drained.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!isLeader() || closed) {
            return CompletableFuture.completedFuture(null);
        }
        for (SegmentInfo s : drained) {
            log.info().attr("segmentId", s.segmentId())
                    .attr("sealedAtMs", s.sealedAtMs())
                    .log("GC: pruning sealed segment past retention");
        }
        return resources.updateScalableTopicAsync(topicName, md -> {
            SegmentLayout latest = SegmentLayout.fromMetadata(md);
            SegmentLayout updated = latest;
            for (SegmentInfo s : drained) {
                // Re-validate per segment: another writer (or a previous failed
                // tick of this same loop) may have already pruned it.
                if (updated.getAllSegments().containsKey(s.segmentId())) {
                    updated = updated.pruneSegment(s.segmentId());
                }
            }
            return updated == latest ? md : updated.toMetadata(md);
        }).thenCompose(__ -> resources.getScalableTopicMetadataAsync(topicName, true))
          .thenCompose(optMd -> {
              currentLayout = SegmentLayout.fromMetadata(optMd.orElseThrow());
              return notifySubscriptions(currentLayout);
          })
          .thenCompose(__ -> {
              CompletableFuture<?>[] deletes = drained.stream()
                      .map(s -> deleteSegmentBackingTopic(s)
                              // The segment is gone from the layout — drop its load record
                              // too, or the .../segments/{id}/load entry leaks forever.
                              .thenCompose(___ ->
                                      resources.deleteSegmentLoadAsync(topicName, s.segmentId())))
                      .toArray(CompletableFuture[]::new);
              return CompletableFuture.allOf(deletes);
          })
          .thenAccept(__ -> {
              for (SegmentInfo s : drained) {
                  log.info().attr("segmentId", s.segmentId())
                          .log("GC: segment pruned + backing topic deleted");
              }
          });
    }

    private CompletableFuture<Boolean> prunable(SegmentInfo seg, List<String> subs) {
        if (subs.isEmpty()) {
            // No subscribers ever attached / all unsubscribed → nothing left to drain.
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean>[] checks = subs.stream()
                .map(sub -> isSegmentDrained(seg, sub))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(checks)
                .thenApply(__ -> {
                    for (CompletableFuture<Boolean> c : checks) {
                        if (!c.join()) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    /**
     * Delete the segment's backing storage via the {@code scalableTopics} admin
     * endpoint, which understands the {@code segment://} naming scheme and routes
     * to the segment's owning broker. Failures are best-effort: the controller
     * has already pruned the segment from the layout (the point of no return),
     * so a failed delete is just leaked storage that the next tick will retry.
     */
    private CompletableFuture<Void> deleteSegmentBackingTopic(SegmentInfo seg) {
        String name = toSegmentPersistentName(seg);
        try {
            return brokerService.getPulsar().getAdminClient()
                    .scalableTopics().deleteSegmentAsync(name, /* force */ true)
                    .exceptionally(ex -> {
                        Throwable cause =
                                org.apache.pulsar.common.util.FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof org.apache.pulsar.client.admin.PulsarAdminException
                                .NotFoundException) {
                            // Already gone — fine.
                            return null;
                        }
                        log.warn().attr("segment", name).exceptionMessage(cause)
                                .log("GC: failed to delete backing segment topic;"
                                        + " will retry on next tick");
                        return null;
                    });
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Resolve the effective retention-time-in-millis for this scalable topic by
     * layering: topic-policy on the parent {@code topic://...} → namespace policy →
     * broker config default. Returns {@code null} if retention is unset or negative
     * (= keep forever) — the GC tick treats that as "skip".
     */
    private CompletableFuture<Long> resolveRetentionMillisAsync() {
        TopicPoliciesService topicPoliciesService =
                brokerService.getPulsar().getTopicPoliciesService();
        // Topic-level (override) layer.
        return topicPoliciesService.getTopicPoliciesAsync(topicName,
                        TopicPoliciesService.GetType.LOCAL_ONLY)
                .thenCompose(localOpt -> {
                    Optional<RetentionPolicies> rp = localOpt
                            .map(TopicPolicies::getRetentionPolicies)
                            .filter(java.util.Objects::nonNull);
                    if (rp.isPresent()) {
                        return CompletableFuture.completedFuture(toRetentionMillis(rp.get()));
                    }
                    // Namespace layer.
                    NamespaceName ns = topicName.getNamespaceObject();
                    return brokerService.getPulsar().getPulsarResources()
                            .getNamespaceResources()
                            .getPoliciesAsync(ns)
                            .thenApply(nsOpt -> {
                                RetentionPolicies nsRp = nsOpt
                                        .map(p -> p.retention_policies)
                                        .orElse(null);
                                if (nsRp != null) {
                                    return toRetentionMillis(nsRp);
                                }
                                return defaultRetentionMillisFromBrokerConfig();
                            });
                });
    }

    private static Long toRetentionMillis(RetentionPolicies rp) {
        if (rp.getRetentionTimeInMinutes() < 0) {
            return null; // keep forever
        }
        return TimeUnit.MINUTES.toMillis(rp.getRetentionTimeInMinutes());
    }

    private Long defaultRetentionMillisFromBrokerConfig() {
        var conf = brokerService.getPulsar().getConfig();
        if (conf == null) {
            return null;
        }
        int min = conf.getDefaultRetentionTimeInMinutes();
        return min < 0 ? null : TimeUnit.MINUTES.toMillis(min);
    }

    /** Test hook: count of sealed segments currently in the layout. */
    int sealedSegmentCount() {
        SegmentLayout layout = currentLayout;
        if (layout == null) {
            return 0;
        }
        int n = 0;
        for (SegmentInfo s : layout.getAllSegments().values()) {
            if (s.isSealed()) {
                n++;
            }
        }
        return n;
    }

    // --- Lifecycle ---

    public CompletableFuture<Void> close() {
        closed = true;
        cancelGcTask();
        cancelAutoScaleTask();
        // Stop each coordinator's drain poller before clearing — otherwise the scheduler
        // task keeps running after the controller goes away.
        subscriptions.values().forEach(SubscriptionCoordinator::close);
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
                                                        int entryBucketBudget,
                                                        Map<String, String> properties) {
        if (numInitialSegments < 1) {
            throw new IllegalArgumentException("Must have at least 1 segment");
        }

        int rangeSize = (HashRange.MAX_HASH + 1) / numInitialSegments;
        Map<Long, SegmentInfo> segments = new LinkedHashMap<>();

        // PIP-486: share the topic's entry-bucket budget equally across the initial segments.
        List<Integer> entryBucketSplits = EntryBucketSplits.equalWidth(
                EntryBucketSplits.bucketsForBudget(entryBucketBudget, numInitialSegments));

        long nowMs = System.currentTimeMillis();
        for (int i = 0; i < numInitialSegments; i++) {
            int start = i * rangeSize;
            int end = (i == numInitialSegments - 1) ? HashRange.MAX_HASH : (start + rangeSize - 1);
            HashRange range = HashRange.of(start, end);
            SegmentInfo segment = SegmentInfo.active(i, range, 0, nowMs)
                    .withEntryBucketSplits(entryBucketSplits);
            segments.put((long) i, segment);
        }

        return ScalableTopicMetadata.builder()
                .epoch(0)
                .nextSegmentId(numInitialSegments)
                .segments(segments)
                .properties(properties != null ? properties : Map.of())
                .build();
    }

    /**
     * Build the initial scalable-topic layout for a regular-to-scalable migration (PIP-475).
     *
     * <p>Each of the {@code partitions} old partitions (or the whole topic, for a
     * non-partitioned source where {@code partitions <= 0}) becomes a <b>sealed legacy
     * parent</b> segment that wraps the existing {@code persistent://...} topic. Alongside
     * them, {@code N} fresh <b>active children</b> are created with equal-width contiguous
     * hash ranges tiling {@code [0x0000, MAX_HASH]} and standard range-based routing.
     *
     * <p>The parents each span the <i>full</i> hash range because v4 partitioned routing
     * ({@code signSafeMod(hash, N)}) scattered keys for any child's range across every
     * partition. Consequently every child lists <i>all</i> parents as predecessors: the
     * subscription controller's drain-before-assign protocol then drains all parents before
     * a consumer is assigned to any child, preserving per-key ordering across the migration.
     *
     * <p>Segment IDs: parents are {@code 0..N-1}, children are {@code N..2N-1},
     * {@code nextSegmentId == 2N}.
     *
     * @param persistentBase the source topic in the {@code persistent://} domain (its
     *                       partitions are {@code persistentBase-partition-K})
     * @param partitions     the source partition count; {@code <= 0} means non-partitioned
     */
    public static ScalableTopicMetadata createMigratedMetadata(TopicName persistentBase,
                                                               int partitions,
                                                               int entryBucketBudget) {
        int n = Math.max(partitions, 1);
        long nowMs = System.currentTimeMillis();
        Map<Long, SegmentInfo> segments = new LinkedHashMap<>();

        // PIP-486: the active children share the topic's entry-bucket budget. The sealed legacy parents
        // take no new writes, so they keep a single bucket (no splits).
        List<Integer> childEntryBucketSplits = EntryBucketSplits.equalWidth(
                EntryBucketSplits.bucketsForBudget(entryBucketBudget, n));

        // Child IDs are N..2N-1; every child lists every parent (full fan-in).
        List<Long> childIds = new ArrayList<>(n);
        for (int j = 0; j < n; j++) {
            childIds.add((long) (n + j));
        }
        List<Long> parentIds = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            parentIds.add((long) k);
        }

        // N sealed legacy parents — the old partitions, each spanning the full hash range.
        for (int k = 0; k < n; k++) {
            String legacyTopic = partitions <= 0
                    ? persistentBase.toString()
                    : persistentBase.getPartition(k).toString();
            SegmentInfo parent = SegmentInfo
                    .activeLegacy(k, HashRange.of(0x0000, HashRange.MAX_HASH), legacyTopic, 0, nowMs)
                    .sealed(0, nowMs, childIds);
            segments.put((long) k, parent);
        }

        // N active range-based children tiling [0x0000, MAX_HASH]; each has all parents.
        int rangeSize = (HashRange.MAX_HASH + 1) / n;
        for (int j = 0; j < n; j++) {
            long segId = n + j;
            int start = j * rangeSize;
            int end = (j == n - 1) ? HashRange.MAX_HASH : (start + rangeSize - 1);
            SegmentInfo child = SegmentInfo.active(segId, HashRange.of(start, end), parentIds, 0, nowMs)
                    .withEntryBucketSplits(childEntryBucketSplits);
            segments.put(segId, child);
        }

        return ScalableTopicMetadata.builder()
                .epoch(0)
                .nextSegmentId(2L * n)
                .segments(segments)
                .properties(Map.of())
                .build();
    }
}
