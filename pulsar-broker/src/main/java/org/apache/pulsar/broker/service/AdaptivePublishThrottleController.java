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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.util.SingleThreadNonConcurrentFixedRateScheduler;

/**
 * Broker-level controller that periodically evaluates system pressure signals
 * (JVM heap usage and per-topic backlog pressure) and drives per-topic
 * {@link AdaptivePublishRateLimiter} instances.
 *
 * <h3>Pressure computation</h3>
 * <pre>
 *   memoryPressure = clamp((heapUsed/maxHeap - lowWm) / (highWm - lowWm), 0, 1)
 *
 *   backlogPressure per topic:
 *     if (quota ≤ 0)  → 0                    (no quota configured)
 *     fraction = backlogBytes / quotaBytes
 *     if (fraction ≤ backlogLowWm)  → 0
 *     if (fraction ≥ backlogHighWm) → 1
 *     else → (fraction - backlogLowWm) / (backlogHighWm - backlogLowWm)
 *
 *   pressureFactor = max(memoryPressure, backlogPressure)
 * </pre>
 *
 * <h3>Rate computation (bounded step)</h3>
 * <pre>
 *   targetFactor = 1.0 - pressureFactor * (1.0 - minRateFactor)
 *   targetMsgRate = max(1, naturalMsgRate * targetFactor)
 *   delta = naturalMsgRate * maxRateChangeFactor
 *   effectiveMsgRate = clamp(currentRate ± delta, towards target)
 * </pre>
 *
 * <h3>Hysteresis</h3>
 * Throttle activates when {@code pressureFactor > 0}.  It is fully released only
 * when {@code pressureFactor == 0}, which requires both memory and backlog pressure
 * to drop below their respective low watermarks.
 *
 * <h3>Observe-only mode</h3>
 * When {@code adaptivePublisherThrottlingObserveOnly=true} (dynamic config) the
 * controller computes everything — pressure factors, target rates, decisions — and
 * emits metrics and OBSERVE-ONLY log lines, but never calls
 * {@link AdaptivePublishRateLimiter#activate} or
 * {@link AdaptivePublishRateLimiter#deactivate}.  This is the safe validation mode
 * before going live, and also serves as an emergency circuit-breaker (flip the
 * dynamic config at runtime to suspend all throttling without a restart).
 */
@Slf4j
public class AdaptivePublishThrottleController implements Closeable {

    private final BrokerService brokerService;
    private final SingleThreadNonConcurrentFixedRateScheduler scheduler;

    /**
     * Current broker-wide memory pressure factor (0.0–1.0).
     * Written by the controller thread; read by metrics/tests.
     */
    @Getter
    private volatile double currentMemoryPressureFactor = 0.0;

    /**
     * Number of topics on this broker currently being adaptively throttled.
     */
    private final AtomicInteger activeThrottledTopicsCount = new AtomicInteger(0);

    /**
     * Total number of throttle activations since the controller started.
     */
    private final AtomicLong totalActivationCount = new AtomicLong(0L);

    /**
     * Unix epoch milliseconds when the last evaluation cycle completed (success or failure).
     * Zero if no cycle has run yet.  Operators can alert when this stops advancing.
     */
    private volatile long lastEvaluationCompletedEpochMs = 0L;

    /**
     * Wall-clock duration in milliseconds of the most recent evaluation cycle.
     * A sustained increase signals the broker has too many topics for the configured interval.
     */
    private volatile long lastEvaluationDurationMs = 0L;

    /**
     * Number of evaluation cycles that threw an uncaught exception.
     * Any increment means the controller is degraded; check broker logs for
     * "[AdaptiveThrottleController] Evaluation cycle failed".
     */
    private final AtomicLong evaluationFailureCount = new AtomicLong(0L);

    public AdaptivePublishThrottleController(BrokerService brokerService) {
        this.brokerService = brokerService;
        this.scheduler =
                new SingleThreadNonConcurrentFixedRateScheduler("pulsar-adaptive-throttle-controller");
    }

    /** Start periodic evaluation. */
    public void start() {
        ServiceConfiguration config = brokerService.getPulsar().getConfiguration();
        long intervalMs = config.getAdaptivePublisherThrottlingIntervalMs();
        log.info("[AdaptiveThrottleController] Starting with intervalMs={} observeOnly={}",
                intervalMs, config.isAdaptivePublisherThrottlingObserveOnly());
        scheduler.scheduleAtFixedRateNonConcurrently(
                this::runControllerCycle, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        log.info("[AdaptiveThrottleController] Shutting down");
        scheduler.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Main control loop
    // -------------------------------------------------------------------------

    @VisibleForTesting
    void runControllerCycle() {
        long cycleStartNs = System.nanoTime();
        try {
            ServiceConfiguration config = brokerService.getPulsar().getConfiguration();
            boolean observeOnly = config.isAdaptivePublisherThrottlingObserveOnly();

            double memoryPressure = computeMemoryPressure(config);
            currentMemoryPressureFactor = memoryPressure;

            int[] counters = {0, 0, 0}; // [evaluated, throttled, newlyActivated]

            brokerService.forEachPersistentTopic(topic -> {
                try {
                    evaluateTopic(topic, config, memoryPressure, observeOnly, counters);
                } catch (Exception e) {
                    log.warn("[AdaptiveThrottleController] Error evaluating topic {}: {}",
                            topic.getName(), e.getMessage(), e);
                }
            });

            activeThrottledTopicsCount.set(counters[1]);

            if (log.isDebugEnabled()) {
                long cycleMs = (System.nanoTime() - cycleStartNs) / 1_000_000;
                log.debug("[AdaptiveThrottleController] cycle complete: "
                                + "topicsEvaluated={} topicsThrottled={} newlyActivated={} "
                                + "memoryPressure={} cycleMs={} observeOnly={}",
                        counters[0], counters[1], counters[2],
                        String.format("%.3f", memoryPressure), cycleMs, observeOnly);
            }
        } catch (Exception e) {
            long failures = evaluationFailureCount.incrementAndGet();
            log.error("[AdaptiveThrottleController] Evaluation cycle failed "
                            + "(totalFailures={}, lastError={}). "
                            + "Controller remains scheduled; check for broker misconfiguration.",
                    failures, e.getMessage(), e);
        } finally {
            lastEvaluationDurationMs = (System.nanoTime() - cycleStartNs) / 1_000_000;
            lastEvaluationCompletedEpochMs = System.currentTimeMillis();
        }
    }

    // -------------------------------------------------------------------------
    // Per-topic evaluation
    // -------------------------------------------------------------------------

    private void evaluateTopic(PersistentTopic topic, ServiceConfiguration config,
                               double memoryPressure, boolean observeOnly, int[] counters) {
        AdaptivePublishRateLimiter limiter = topic.getAdaptivePublishRateLimiter();
        if (limiter == null) {
            return;
        }

        counters[0]++;
        long nowNs = System.nanoTime();

        double backlogPressure = computeBacklogPressure(topic, config);
        double pressureFactor = Math.max(memoryPressure, backlogPressure);

        if (pressureFactor <= 0.0) {
            // No pressure: sample natural rate and deactivate if needed.
            boolean wasActive = limiter.isActive();
            limiter.sampleRate(topic.getMsgInCounter(), topic.getBytesInCounter(), nowNs);
            if (wasActive) {
                maybeLogDeactivated(topic, limiter, nowNs, observeOnly);
                if (!observeOnly) {
                    limiter.deactivate();
                }
            }
        } else {
            counters[1]++;

            double naturalMsgRate = limiter.getNaturalMsgRateEstimate();
            double naturalByteRate = limiter.getNaturalByteRateEstimate();

            if (naturalMsgRate <= 0.0 && naturalByteRate <= 0.0) {
                // No baseline yet — sample now (first cycle after topic load).
                limiter.sampleRate(topic.getMsgInCounter(), topic.getBytesInCounter(), nowNs);
                if (log.isDebugEnabled()) {
                    log.debug("[AdaptiveThrottle] Skipping activation: no natural rate baseline yet for {}",
                            topic.getName());
                }
                return;
            }

            long targetMsgRate = computeTargetRate(naturalMsgRate, pressureFactor,
                    limiter.getCurrentEffectiveMsgRate(), config);
            long targetByteRate = computeTargetRate(naturalByteRate, pressureFactor,
                    limiter.getCurrentEffectiveByteRate(), config);

            boolean wasActive = limiter.isActive();

            // Check if rate was clamped to minimum floor
            double minFactor = config.getAdaptivePublisherThrottlingMinRateFactor();
            boolean clampedByMin = naturalMsgRate > 0
                    && targetMsgRate <= (long) Math.ceil(naturalMsgRate * minFactor * 1.05);

            if (observeOnly) {
                logObserveOnly(topic, limiter, naturalMsgRate, naturalByteRate,
                        targetMsgRate, targetByteRate, memoryPressure, backlogPressure,
                        pressureFactor, config);
            } else {
                limiter.activate(targetMsgRate, targetByteRate);
                if (!wasActive) {
                    counters[2]++;
                    totalActivationCount.incrementAndGet();
                    logActivated(topic, limiter, naturalMsgRate, naturalByteRate,
                            targetMsgRate, targetByteRate, memoryPressure, backlogPressure,
                            pressureFactor, config, nowNs);
                } else if (clampedByMin && log.isWarnEnabled()) {
                    log.warn("[AdaptiveThrottle] Rate clamped at minimum floor: "
                                    + "topic={} computedFactor={} minRateFactor={} "
                                    + "effectiveMsgRate={} naturalMsgRate={} "
                                    + "backlogPressure={} memoryPressure={}",
                            topic.getName(),
                            String.format("%.3f", 1.0 - pressureFactor * (1.0 - minFactor)),
                            minFactor, targetMsgRate, (long) naturalMsgRate,
                            String.format("%.3f", backlogPressure),
                            String.format("%.3f", memoryPressure));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Pressure computation
    // -------------------------------------------------------------------------

    @VisibleForTesting
    double computeMemoryPressure(ServiceConfiguration config) {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        if (maxMemory == Long.MAX_VALUE) {
            // Unbounded heap — cannot compute pressure.
            return 0.0;
        }
        long usedMemory = maxMemory - runtime.freeMemory();
        double usageFraction = (double) usedMemory / maxMemory;

        double highWm = config.getAdaptivePublisherThrottlingMemoryHighWatermarkPct();
        double lowWm = config.getAdaptivePublisherThrottlingMemoryLowWatermarkPct();
        return linearPressure(usageFraction, lowWm, highWm);
    }

    @VisibleForTesting
    double computeBacklogPressure(PersistentTopic topic, ServiceConfiguration config) {
        long quotaLimit =
                topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize();
        if (quotaLimit <= 0) {
            return 0.0;
        }
        long backlogSize = topic.getBacklogSize();
        double fraction = (double) backlogSize / quotaLimit;

        double highWm = config.getAdaptivePublisherThrottlingBacklogHighWatermarkPct();
        double lowWm = config.getAdaptivePublisherThrottlingBacklogLowWatermarkPct();
        return linearPressure(fraction, lowWm, highWm);
    }

    /**
     * Linear pressure ramp: 0 below {@code lowWm}, 1 above {@code highWm},
     * linear interpolation in between.
     */
    @VisibleForTesting
    static double linearPressure(double value, double lowWm, double highWm) {
        if (value <= lowWm) {
            return 0.0;
        }
        if (value >= highWm) {
            return 1.0;
        }
        double range = highWm - lowWm;
        return range <= 0.0 ? 1.0 : (value - lowWm) / range;
    }

    // -------------------------------------------------------------------------
    // Rate computation (bounded step)
    // -------------------------------------------------------------------------

    /**
     * Compute the new effective rate for one dimension (msg or bytes) using
     * bounded-step logic to prevent sudden rate changes.
     *
     * @param naturalRate    observed natural rate (unthrottled)
     * @param pressureFactor combined pressure factor in [0, 1]
     * @param currentRate    the effective rate from the previous cycle (0 = inactive)
     * @param config         broker configuration
     * @return new effective rate (≥ 1)
     */
    @VisibleForTesting
    long computeTargetRate(double naturalRate, double pressureFactor,
                           long currentRate, ServiceConfiguration config) {
        if (naturalRate <= 0.0) {
            return 1L;
        }
        double minFactor = config.getAdaptivePublisherThrottlingMinRateFactor();
        double maxChangeFactor = config.getAdaptivePublisherThrottlingMaxRateChangeFactor();

        // Where we want to be
        double targetFactor = 1.0 - pressureFactor * (1.0 - minFactor);
        long idealTarget = Math.max(1L, (long) (naturalRate * targetFactor));

        // Maximum allowed change this cycle
        long maxDelta = Math.max(1L, (long) (naturalRate * maxChangeFactor));

        // Start from the previous effective rate (or natural rate if just activating)
        long base = (currentRate > 0) ? currentRate : (long) naturalRate;

        long newRate;
        if (idealTarget < base) {
            // Ramping down
            newRate = Math.max(idealTarget, base - maxDelta);
        } else {
            // Ramping up (or steady)
            newRate = Math.min(idealTarget, base + maxDelta);
        }

        // Hard floor
        long minAbsolute = Math.max(1L, (long) (naturalRate * minFactor));
        return Math.max(newRate, minAbsolute);
    }

    // -------------------------------------------------------------------------
    // Logging helpers
    // -------------------------------------------------------------------------

    private void logActivated(PersistentTopic topic, AdaptivePublishRateLimiter limiter,
                               double naturalMsgRate, double naturalByteRate,
                               long targetMsgRate, long targetByteRate,
                               double memoryPressure, double backlogPressure,
                               double pressureFactor, ServiceConfiguration config,
                               long nowNs) {
        if (!limiter.shouldLogTransition(nowNs)) {
            return;
        }
        long backlogBytes = topic.getBacklogSize();
        long quotaBytes = topic.getBacklogQuota(
                BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize();
        log.info("[AdaptiveThrottle] ACTIVATED topic={} "
                        + "naturalMsgRate={}/s naturalByteRate={}/s "
                        + "effectiveMsgRate={}/s effectiveByteRate={}/s "
                        + "rateFactor={} pressureFactor={} "
                        + "memoryPressure={} backlogPressure={} "
                        + "backlogBytes={} backlogQuotaBytes={} backlogPct={} "
                        + "minRateFactor={} maxRateChangeFactor={} observeOnly=false",
                topic.getName(),
                String.format("%.1f", naturalMsgRate),
                String.format("%.0f", naturalByteRate),
                targetMsgRate, targetByteRate,
                String.format("%.3f", 1.0 - pressureFactor
                        * (1.0 - config.getAdaptivePublisherThrottlingMinRateFactor())),
                String.format("%.3f", pressureFactor),
                String.format("%.3f", memoryPressure),
                String.format("%.3f", backlogPressure),
                backlogBytes, quotaBytes,
                quotaBytes > 0 ? String.format("%.1f%%", 100.0 * backlogBytes / quotaBytes) : "N/A",
                config.getAdaptivePublisherThrottlingMinRateFactor(),
                config.getAdaptivePublisherThrottlingMaxRateChangeFactor());
    }

    private void maybeLogDeactivated(PersistentTopic topic, AdaptivePublishRateLimiter limiter,
                                     long nowNs, boolean observeOnly) {
        if (!limiter.shouldLogTransition(nowNs)) {
            return;
        }
        log.info("[AdaptiveThrottle] {} topic={} "
                        + "naturalMsgRate={}/s minEffectiveMsgRate={}/s "
                        + "activationCount={}",
                observeOnly ? "OBSERVE-ONLY would-deactivate:" : "DEACTIVATED",
                topic.getName(),
                String.format("%.1f", limiter.getNaturalMsgRateEstimate()),
                limiter.getCurrentEffectiveMsgRate(),
                limiter.getThrottleActivationCount());
    }

    private void logObserveOnly(PersistentTopic topic, AdaptivePublishRateLimiter limiter,
                                double naturalMsgRate, double naturalByteRate,
                                long targetMsgRate, long targetByteRate,
                                double memoryPressure, double backlogPressure,
                                double pressureFactor, ServiceConfiguration config) {
        log.info("[AdaptiveThrottle] OBSERVE-ONLY would-activate: topic={} "
                        + "wouldSetMsgRate={}/s wouldSetByteRate={}/s "
                        + "naturalMsgRate={}/s pressureFactor={} "
                        + "memoryPressure={} backlogPressure={} "
                        + "[no actual throttling applied]",
                topic.getName(),
                targetMsgRate, targetByteRate,
                String.format("%.1f", naturalMsgRate),
                String.format("%.3f", pressureFactor),
                String.format("%.3f", memoryPressure),
                String.format("%.3f", backlogPressure));
    }

    // -------------------------------------------------------------------------
    // Metrics accessors (used by OpenTelemetryAdaptiveThrottleStats)
    // -------------------------------------------------------------------------

    public int getActiveThrottledTopicsCount() {
        return activeThrottledTopicsCount.get();
    }

    public long getTotalActivationCount() {
        return totalActivationCount.get();
    }

    /**
     * Unix epoch milliseconds when the last evaluation cycle completed.
     * Returns 0 if no cycle has run yet.  Alert when
     * {@code now - lastEvaluationCompletedEpochMs > 3 * intervalMs}.
     */
    public long getLastEvaluationCompletedEpochMs() {
        return lastEvaluationCompletedEpochMs;
    }

    /**
     * Duration in milliseconds of the most recent evaluation cycle.
     * Alert when this exceeds {@code adaptivePublisherThrottlingIntervalMs} consistently.
     */
    public long getLastEvaluationDurationMs() {
        return lastEvaluationDurationMs;
    }

    /**
     * Total number of evaluation cycles that threw an uncaught exception since start.
     * Any non-zero value means the controller is degraded; check broker logs.
     */
    public long getEvaluationFailureCount() {
        return evaluationFailureCount.get();
    }
}
