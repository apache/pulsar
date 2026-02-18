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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;

/**
 * A per-topic publish rate limiter whose effective rate is controlled dynamically by
 * {@link AdaptivePublishThrottleController}.
 *
 * <p>The limiter wraps a {@link PublishRateLimiterImpl} delegate. When inactive
 * ({@code active == false}), {@link #handlePublishThrottling} is a complete no-op with
 * no lock acquisition and no token-bucket access.  Only the controller (running on its
 * own single-threaded scheduler) calls {@link #activate} and {@link #deactivate}; the
 * IO threads only ever call {@link #handlePublishThrottling}.
 *
 * <h3>Thread-safety model</h3>
 * <ul>
 *   <li>{@code active} — {@code volatile boolean}: written by the controller thread,
 *       read by IO threads; guaranteed visibility without locking.</li>
 *   <li>{@code naturalMsgRateEstimate}, {@code naturalByteRateEstimate},
 *       {@code currentEffectiveMsgRate}, {@code currentEffectiveByteRate},
 *       {@code lastSampleMsgCount}, {@code lastSampleByteCount},
 *       {@code lastSampleTimeNs} — all {@code volatile}; single writer
 *       (controller thread), multiple readers.  No atomic compare-and-set needed
 *       because the controller runs on a single-threaded scheduler.</li>
 *   <li>{@code delegate.update()} — calls into {@link PublishRateLimiterImpl} which
 *       stores token buckets in {@code volatile} fields; safe across threads.</li>
 * </ul>
 */
@Slf4j
public class AdaptivePublishRateLimiter implements PublishRateLimiter {

    /** Inner delegate that owns the actual token buckets. */
    private final PublishRateLimiterImpl delegate;

    /**
     * True when throttling is currently active.  IO threads read this on every
     * publish; the controller writes it.  {@code volatile} guarantees visibility.
     */
    private volatile boolean active = false;

    /**
     * Estimated "natural" (unthrottled) publish rate — updated only while
     * {@code active == false} so it never reflects a post-throttle reduced rate.
     */
    private volatile double naturalMsgRateEstimate = 0.0;
    private volatile double naturalByteRateEstimate = 0.0;

    /**
     * The effective rate currently applied to the token bucket (msg/s).
     * Tracks the smoothed value across controller cycles (bounded step).
     * Zero when not throttling.
     */
    private volatile long currentEffectiveMsgRate = 0L;
    private volatile long currentEffectiveByteRate = 0L;

    /** Cumulative counters captured at the previous sample cycle. */
    private volatile long lastSampleMsgCount = -1L;
    private volatile long lastSampleByteCount = -1L;
    private volatile long lastSampleTimeNs = 0L;

    /**
     * Nanosecond timestamp of the last ACTIVATED→DEACTIVATED transition log, used
     * to suppress repeated log spam for oscillating topics.
     */
    private volatile long lastTransitionLogNs = 0L;

    /** Monotonically increasing count of activate() calls (for metrics). */
    private final LongAdder throttleActivationCount = new LongAdder();

    /** Minimum log re-emission interval for transition events: 60 seconds. */
    private static final long MIN_TRANSITION_LOG_INTERVAL_NS = 60_000_000_000L;

    public AdaptivePublishRateLimiter(MonotonicClock clock,
                                      Consumer<Producer> throttleAction,
                                      Consumer<Producer> unthrottleAction) {
        this.delegate = new PublishRateLimiterImpl(clock, throttleAction, unthrottleAction);
    }

    // -------------------------------------------------------------------------
    // PublishRateLimiter interface — called from IO threads
    // -------------------------------------------------------------------------

    @Override
    public void handlePublishThrottling(Producer producer, int numOfMessages, long msgSizeInBytes) {
        // Fast path: no volatile read beyond the boolean check when inactive.
        if (active) {
            delegate.handlePublishThrottling(producer, numOfMessages, msgSizeInBytes);
        }
    }

    /**
     * Not used: this limiter's rate is controlled exclusively by
     * {@link AdaptivePublishThrottleController}, not by policy updates.
     */
    @Override
    public void update(Policies policies, String clusterName) {
        // intentional no-op
    }

    /**
     * Not used: see {@link #update(Policies, String)}.
     */
    @Override
    public void update(PublishRate maxPublishRate) {
        // intentional no-op
    }

    // -------------------------------------------------------------------------
    // Controller API — called only from the single-threaded controller scheduler
    // -------------------------------------------------------------------------

    /**
     * Sample the instantaneous publish rate from cumulative counters and update
     * the natural rate estimate via asymmetric EWMA.  Must be called only when
     * {@code active == false} so the estimate reflects the unthrottled rate.
     *
     * @param currentMsgCount  current value of AbstractTopic.getMsgInCounter()
     * @param currentByteCount current value of AbstractTopic.getBytesInCounter()
     * @param nowNs            current time in nanoseconds (from System.nanoTime())
     */
    public void sampleRate(long currentMsgCount, long currentByteCount, long nowNs) {
        if (lastSampleTimeNs > 0 && nowNs > lastSampleTimeNs) {
            double elapsedSec = (nowNs - lastSampleTimeNs) / 1e9;
            if (elapsedSec > 0.0) {
                double instantMsgRate = Math.max(0.0, (currentMsgCount - lastSampleMsgCount) / elapsedSec);
                double instantByteRate = Math.max(0.0, (currentByteCount - lastSampleByteCount) / elapsedSec);
                updateNaturalRateEstimate(instantMsgRate, instantByteRate);
            }
        }
        lastSampleMsgCount = currentMsgCount;
        lastSampleByteCount = currentByteCount;
        lastSampleTimeNs = nowNs;
    }

    /**
     * Activate throttling at the given rates.  The delegate's token buckets are
     * replaced atomically (volatile writes inside PublishRateLimiterImpl).
     * If already active, this updates the current effective rate.
     *
     * @param msgRatePerSec  target message rate (> 0)
     * @param byteRatePerSec target byte rate (> 0, or 0 to leave byte bucket absent)
     */
    public void activate(long msgRatePerSec, long byteRatePerSec) {
        boolean wasActive = active;
        active = true;
        currentEffectiveMsgRate = msgRatePerSec;
        currentEffectiveByteRate = byteRatePerSec;
        delegate.update(new PublishRate((int) Math.min(msgRatePerSec, Integer.MAX_VALUE), byteRatePerSec));
        if (!wasActive) {
            throttleActivationCount.increment();
        }
    }

    /**
     * Deactivate throttling.  The token buckets are cleared immediately, allowing
     * all subsequent publish requests to pass through.
     */
    public void deactivate() {
        active = false;
        currentEffectiveMsgRate = 0L;
        currentEffectiveByteRate = 0L;
        delegate.update((PublishRate) null);
    }

    // -------------------------------------------------------------------------
    // Accessors used by the controller and metrics
    // -------------------------------------------------------------------------

    public boolean isActive() {
        return active;
    }

    public double getNaturalMsgRateEstimate() {
        return naturalMsgRateEstimate;
    }

    public double getNaturalByteRateEstimate() {
        return naturalByteRateEstimate;
    }

    public long getCurrentEffectiveMsgRate() {
        return currentEffectiveMsgRate;
    }

    public long getCurrentEffectiveByteRate() {
        return currentEffectiveByteRate;
    }

    public long getThrottleActivationCount() {
        return throttleActivationCount.sum();
    }

    /**
     * Returns true if this topic's transition log should be emitted (rate-limits
     * repeated transition logs for oscillating topics to at most once per minute).
     */
    public boolean shouldLogTransition(long nowNs) {
        if (nowNs - lastTransitionLogNs >= MIN_TRANSITION_LOG_INTERVAL_NS) {
            lastTransitionLogNs = nowNs;
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Asymmetric EWMA: tracks increases faster (alpha=0.30) than decreases
     * (alpha=0.05).  This captures the producer's peak desire rate rather than
     * decaying quickly when they momentarily slow down.
     */
    private void updateNaturalRateEstimate(double instantMsgRate, double instantByteRate) {
        if (instantMsgRate > naturalMsgRateEstimate) {
            naturalMsgRateEstimate = naturalMsgRateEstimate * 0.70 + instantMsgRate * 0.30;
        } else {
            naturalMsgRateEstimate = naturalMsgRateEstimate * 0.95 + instantMsgRate * 0.05;
        }
        if (instantByteRate > naturalByteRateEstimate) {
            naturalByteRateEstimate = naturalByteRateEstimate * 0.70 + instantByteRate * 0.30;
        } else {
            naturalByteRateEstimate = naturalByteRateEstimate * 0.95 + instantByteRate * 0.05;
        }
    }

    @VisibleForTesting
    public PublishRateLimiterImpl getDelegate() {
        return delegate;
    }
}
