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

import static java.util.Objects.requireNonNull;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;

/**
 * Accounts for all retained inbound roots in a broker and controls its shared request-frame limiter.
 * All connections use the latest dynamically configured limit and the same memory counter.
 * A striped counter reduces contention across I/O and release threads. Threshold decisions accept approximate
 * snapshots; while throttled, periodic recovery checks prevent a missed release from leaving connections paused.
 * State transitions and registration take the write lock; ordinary policy checks use an optimistic read.
 * Callbacks always run outside the lock and production notifications are coalesced on each connection event loop.
 */
@CustomLog
final class InProgressRequestFrameMemoryController implements AutoCloseable {
    private static final int ENABLE_DIRECT_MEMORY_USAGE_PERCENT = 85;
    private static final int DISABLE_DIRECT_MEMORY_USAGE_PERCENT = 65;

    private static final long RECOVERY_CHECK_INTERVAL_MILLIS = 300;

    // sum() can overestimate or underestimate concurrent usage. This is a soft limit, not a reservation;
    // root ownership still uses an exact reference count so every capacity is returned exactly once.
    private final LongAdder retainedFrameBytes = new LongAdder();
    private final ScheduledExecutorService recoveryExecutor;
    // Owned by stateLock together with throttle transitions; the executor belongs to PulsarService.
    private ScheduledFuture<?> recoveryTask;
    private volatile boolean closed;
    private final StampedLock stateLock = new StampedLock();
    private final Map<EventExecutor, InProgressRequestFrameMemoryNotifier> eventLoopNotifiers = new HashMap<>();
    private final Map<Object, Runnable> throttleStateListeners = new HashMap<>();
    private final LongSupplier directMemoryUsedBytesSupplier;
    private final long maxDirectMemoryBytes;
    private final InProgressRequestFrameMemoryLimiter limiter;
    private final InProgressRequestFrameMemoryLimitMode mode;
    private boolean automaticallyEnabled;
    private long automaticEnableThresholdBytes;

    InProgressRequestFrameMemoryController(ScheduledExecutorService recoveryExecutor,
                                          InProgressRequestFrameMemoryLimitMode mode, long limitBytes,
                                          LongSupplier directMemoryUsedBytesSupplier, long maxDirectMemoryBytes) {
        this.mode = requireNonNull(mode);
        this.recoveryExecutor = requireNonNull(recoveryExecutor);
        this.directMemoryUsedBytesSupplier = directMemoryUsedBytesSupplier;
        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
        automaticEnableThresholdBytes = automaticEnableThresholdBytes(limitBytes);
        limiter = new InProgressRequestFrameMemoryLimiter(limitBytes, this);
    }

    InProgressRequestFrameMemoryLimiter getLimiter() {
        return limiter;
    }

    long getRetainedFrameBytes() {
        return retainedFrameBytes.sum();
    }

    boolean isTrackingOnly() {
        return mode == InProgressRequestFrameMemoryLimitMode.TRACKING;
    }

    @VisibleForTesting
    int getRegisteredListenerCount() {
        long stamp = stateLock.readLock();
        try {
            return throttleStateListeners.size();
        } finally {
            stateLock.unlockRead(stamp);
        }
    }

    boolean isEnabled() {
        long stamp = stateLock.readLock();
        try {
            return !closed && (mode == InProgressRequestFrameMemoryLimitMode.ENABLED || automaticallyEnabled);
        } finally {
            stateLock.unlockRead(stamp);
        }
    }

    boolean isAutomaticallyEnabled() {
        long stamp = stateLock.readLock();
        try {
            return automaticallyEnabled;
        } finally {
            stateLock.unlockRead(stamp);
        }
    }

    InProgressRequestFrameMemoryNotifier getEventLoopNotifier(EventExecutor executor) {
        long stamp = stateLock.writeLock();
        try {
            return eventLoopNotifiers.computeIfAbsent(executor,
                    eventLoop -> new InProgressRequestFrameMemoryNotifier(this, eventLoop));
        } finally {
            stateLock.unlockWrite(stamp);
        }
    }

    void removeEventLoopNotifier(EventExecutor executor, InProgressRequestFrameMemoryNotifier notifier) {
        long stamp = stateLock.writeLock();
        try {
            eventLoopNotifiers.remove(executor, notifier);
        } finally {
            stateLock.unlockWrite(stamp);
        }
    }

    void acquire(long bytes) {
        retainedFrameBytes.add(bytes);
    }

    void release(long bytes) {
        retainedFrameBytes.add(-bytes);
        // Usually resume immediately. A concurrent sum can miss the final release; the throttled-only task
        // retries even when paused connections cannot supply another inbound read. TRACKING skips all checks.
        updateState(false);
    }

    void onInboundRead() {
        updateState(true);
    }

    private void updateState(boolean inboundRead) {
        // The startup-only mode is immutable: metrics-only accounting never needs policy synchronization,
        // even when the numeric limit changes. AUTO still checks pressure and releases must still resume reads.
        if (isTrackingOnly() || closed) {
            return;
        }
        long stamp = stateLock.tryOptimisticRead();
        boolean enabled = mode == InProgressRequestFrameMemoryLimitMode.ENABLED || automaticallyEnabled;
        long usedBytes = getRetainedFrameBytes();
        boolean checkAutomaticMode = mode == InProgressRequestFrameMemoryLimitMode.AUTO
                && maxDirectMemoryBytes > 0
                && (automaticallyEnabled || inboundRead && automaticEnableThresholdBytes > 0
                        && usedBytes > automaticEnableThresholdBytes);
        long limitBytes = limiter.getLimitBytes();
        boolean checkThrottle = enabled && limitBytes > 0 && (limiter.isThrottled()
                ? usedBytes <= limitBytes / 2 : inboundRead && usedBytes > limitBytes);
        long directMemoryUsedBytes = checkAutomaticMode && stamp != 0
                ? directMemoryUsedBytesSupplier.getAsLong() : -1;
        boolean automaticModeMayChange = checkAutomaticMode && directMemoryUsedBytes >= 0
                && (automaticallyEnabled
                        ? directMemoryUsedBytes < percentageOf(maxDirectMemoryBytes,
                                DISABLE_DIRECT_MEMORY_USAGE_PERCENT)
                        : directMemoryUsedBytes >= percentageOf(maxDirectMemoryBytes,
                                ENABLE_DIRECT_MEMORY_USAGE_PERCENT));
        if (stateLock.validate(stamp)) {
            if (!automaticModeMayChange && !checkThrottle) {
                return;
            }
        } else {
            // Configuration or a throttle transition raced with sampling. Recheck the policy before sampling
            // again; steady automatic mode still needs a memory sample, but does not need the write lock.
            directMemoryUsedBytes = sampleDirectMemoryUsage(inboundRead);
        }
        AutomaticStateChange automaticStateChange;
        List<Runnable> notifications;
        stamp = stateLock.writeLock();
        try {
            if (closed) {
                return;
            }
            boolean previouslyEnabled = mode == InProgressRequestFrameMemoryLimitMode.ENABLED || automaticallyEnabled;
            automaticStateChange = updateAutomaticLimitEnabledState(directMemoryUsedBytes, inboundRead);
            // Automatic activation only enables the policy; the next inbound read enforces it. Releases may
            // resume connections, but never start throttling. Read usage inside the write lock, not before it.
            notifications = updateThrottleState(inboundRead && previouslyEnabled)
                    ? new ArrayList<>(throttleStateListeners.values()) : List.of();
        } finally {
            stateLock.unlockWrite(stamp);
        }
        logAutomaticStateChange(automaticStateChange);
        notifyThrottleStateListeners(notifications);
    }

    void updateLimitBytes(long limitBytes) {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            if (closed || limiter.getLimitBytes() == limitBytes) {
                return;
            }
            automaticEnableThresholdBytes = automaticEnableThresholdBytes(limitBytes);
            if (limitBytes <= 0) {
                automaticallyEnabled = false;
            }
            // Keep the same limiter for existing and future connections. Re-evaluate retained memory now:
            // a paused connection cannot provide a new read to observe a raised or disabled limit.
            limiter.setLimitBytes(limitBytes);
            notifications = updateThrottleState(true) ? new ArrayList<>(throttleStateListeners.values()) : List.of();
        } finally {
            stateLock.unlockWrite(stamp);
        }
        notifyThrottleStateListeners(notifications);
    }

    void registerThrottleStateListener(Object key, Runnable listener) {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            if (closed) {
                return;
            }
            throttleStateListeners.put(key, listener);
            boolean manuallyEnabled = mode == InProgressRequestFrameMemoryLimitMode.ENABLED;
            if (updateThrottleState(manuallyEnabled)) {
                notifications = new ArrayList<>(throttleStateListeners.values());
            } else {
                notifications = limiter.isThrottled() ? List.of(listener) : List.of();
            }
        } finally {
            stateLock.unlockWrite(stamp);
        }
        notifyThrottleStateListeners(notifications);
    }

    void unregisterThrottleStateListener(Object key) {
        long stamp = stateLock.writeLock();
        try {
            throttleStateListeners.remove(key);
        } finally {
            stateLock.unlockWrite(stamp);
        }
    }

    // Called under the write lock for every transition, including configuration changes and new listeners.
    private boolean updateThrottleState(boolean allowNewThrottle) {
        boolean previouslyThrottled = limiter.isThrottled();
        boolean enabled = !closed
                && (mode == InProgressRequestFrameMemoryLimitMode.ENABLED || automaticallyEnabled);
        limiter.updateThrottledState(enabled, enabled ? getRetainedFrameBytes() : 0, allowNewThrottle);
        if (limiter.isThrottled()) {
            if (recoveryTask == null) {
                try {
                    recoveryTask = recoveryExecutor.scheduleWithFixedDelay(this::checkRecovery,
                            RECOVERY_CHECK_INTERVAL_MILLIS, RECOVERY_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException error) {
                    // Never pause connections without the recovery fallback, including during executor shutdown.
                    limiter.updateThrottledState(false, 0, false);
                    log.warn().exception(error).log("Unable to schedule request-frame memory recovery checks");
                    // A decoder may have observed the briefly published throttle flag. Notify it to resume.
                    return true;
                }
            }
        } else if (recoveryTask != null) {
            recoveryTask.cancel(false);
            recoveryTask = null;
        }
        return previouslyThrottled != limiter.isThrottled();
    }

    private void checkRecovery() {
        try {
            if (!closed && limiter.isThrottled()) {
                // Recovery only: this task must not activate AUTO or start another throttle period. It runs
                // only while reads are paused; ordinary inbound/release events still handle policy changes.
                updateState(false);
            }
        } catch (RuntimeException error) {
            // Scheduled executors suppress subsequent executions after an exception. Keep the fallback alive.
            log.warn().exception(error).log("Failed to check request-frame memory recovery");
        }
    }

    @Override
    public void close() {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            closed = true;
            automaticallyEnabled = false;
            notifications = updateThrottleState(false) ? new ArrayList<>(throttleStateListeners.values()) : List.of();
            throttleStateListeners.clear();
            eventLoopNotifiers.clear();
        } finally {
            stateLock.unlockWrite(stamp);
        }
        // Outstanding roots can still release their capacity after shutdown; no task can be restarted.
        notifyThrottleStateListeners(notifications);
    }

    private long sampleDirectMemoryUsage(boolean allowEnable) {
        long stamp = stateLock.readLock();
        try {
            if (closed || mode != InProgressRequestFrameMemoryLimitMode.AUTO
                    || maxDirectMemoryBytes <= 0 || !automaticallyEnabled
                    && (!allowEnable || automaticEnableThresholdBytes <= 0
                            || getRetainedFrameBytes() <= automaticEnableThresholdBytes)) {
                return -1;
            }
        } finally {
            stateLock.unlockRead(stamp);
        }
        return directMemoryUsedBytesSupplier.getAsLong();
    }

    private AutomaticStateChange updateAutomaticLimitEnabledState(long directMemoryUsedBytes, boolean allowEnable) {
        if (mode != InProgressRequestFrameMemoryLimitMode.AUTO || directMemoryUsedBytes < 0
                || maxDirectMemoryBytes <= 0) {
            return null;
        }
        boolean previouslyEnabled = automaticallyEnabled;
        long usedBytes = getRetainedFrameBytes();
        if (automaticallyEnabled) {
            if (directMemoryUsedBytes < percentageOf(maxDirectMemoryBytes, DISABLE_DIRECT_MEMORY_USAGE_PERCENT)) {
                automaticallyEnabled = false;
            }
        } else if (allowEnable && automaticEnableThresholdBytes > 0
                && directMemoryUsedBytes >= percentageOf(maxDirectMemoryBytes, ENABLE_DIRECT_MEMORY_USAGE_PERCENT)
                && usedBytes > automaticEnableThresholdBytes) {
            automaticallyEnabled = true;
        }
        return previouslyEnabled == automaticallyEnabled ? null : new AutomaticStateChange(automaticallyEnabled,
                directMemoryUsedBytes, usedBytes, automaticEnableThresholdBytes);
    }

    private void logAutomaticStateChange(AutomaticStateChange change) {
        if (change != null) {
            log.info().attr("enabled", change.enabled()).attr("directMemoryUsedBytes", change.directMemoryUsedBytes())
                    .attr("maxDirectMemoryBytes", maxDirectMemoryBytes)
                    .attr("retainedFrameBytes", change.retainedFrameBytes())
                    .attr("automaticEnableThresholdBytes", change.enableThresholdBytes())
                    .log("Automatically updated in-progress request-frame memory limiting");
        }
    }

    private static void notifyThrottleStateListeners(List<Runnable> listeners) {
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException error) {
                log.warn().exception(error).log("Failed to update request-frame memory throttling for a connection");
            }
        }
    }

    static long automaticEnableThresholdBytes(long limitBytes) {
        if (limitBytes <= 0) {
            return limitBytes;
        }
        long halfLimitBytes = limitBytes / 2;
        return limitBytes > Long.MAX_VALUE - halfLimitBytes ? Long.MAX_VALUE : limitBytes + halfLimitBytes;
    }

    private static long percentageOf(long value, int percentage) {
        return value / 100 * percentage + value % 100 * percentage / 100;
    }

    private record AutomaticStateChange(boolean enabled, long directMemoryUsedBytes,
                                        long retainedFrameBytes, long enableThresholdBytes) { }
}
