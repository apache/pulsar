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
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongSupplier;
import lombok.CustomLog;

/**
 * Accounts for all retained inbound roots in a broker and controls its request-frame limiters.
 * Each configuration generation has an immutable limit, but all generations use the same memory counter.
 * State transitions and registration take the write lock; ordinary accounting and reads use an optimistic check.
 * Callbacks always run outside the lock and production notifications are coalesced on each connection event loop.
 */
@CustomLog
final class InProgressRequestFrameMemoryController {
    private static final int ENABLE_DIRECT_MEMORY_USAGE_PERCENT = 85;
    private static final int DISABLE_DIRECT_MEMORY_USAGE_PERCENT = 65;

    private final AtomicLong retainedFrameBytes = new AtomicLong();
    private final StampedLock stateLock = new StampedLock();
    private final Map<EventExecutor, InProgressRequestFrameMemoryNotifier> eventLoopNotifiers = new HashMap<>();
    // Summaries of registered generations, maintained under the write lock. Optimistic readers inspect only
    // these scalars, never the mutable listener maps, and validate their stamp before skipping a state update.
    private long minimumUnthrottledLimit = Long.MAX_VALUE;
    private long maximumResumeThreshold = -1;
    private final Map<InProgressRequestFrameMemoryLimiter, Map<Object, Runnable>> throttleStateListeners =
            new HashMap<>();
    private final LongSupplier directMemoryUsedBytesSupplier;
    private final long maxDirectMemoryBytes;
    private volatile InProgressRequestFrameMemoryLimiter currentLimiter;
    private boolean userEnabled;
    private boolean automaticallyEnabled;
    private long automaticEnableThresholdBytes;

    InProgressRequestFrameMemoryController(boolean userEnabled, long limitBytes,
                                          LongSupplier directMemoryUsedBytesSupplier, long maxDirectMemoryBytes) {
        this.userEnabled = userEnabled;
        this.directMemoryUsedBytesSupplier = directMemoryUsedBytesSupplier;
        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
        automaticEnableThresholdBytes = automaticEnableThresholdBytes(limitBytes);
        currentLimiter = new InProgressRequestFrameMemoryLimiter(limitBytes, this);
    }

    InProgressRequestFrameMemoryLimiter getCurrentLimiter() {
        return currentLimiter;
    }

    long getRetainedFrameBytes() {
        return retainedFrameBytes.get();
    }

    @VisibleForTesting
    int getRegisteredLimiterCount() {
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
            return userEnabled || automaticallyEnabled;
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
        retainedFrameBytes.addAndGet(bytes);
    }

    void release(long bytes) {
        // Subtract before reading the stamp. A concurrent writer must either see the lower usage, or invalidate
        // our optimistic read so that we recheck after its transition. Otherwise the last release could miss
        // a writer about to pause connections, leaving them paused with no future event available to resume them.
        retainedFrameBytes.addAndGet(-bytes);
        updateState(false);
        // No periodic task is needed: new throttling only affects subsequent inbound reads. Root releases must
        // still trigger recovery, since a paused connection cannot supply the next inbound event itself.
    }

    void onInboundRead() {
        updateState(true);
    }

    private void updateState(boolean inboundRead) {
        long stamp = stateLock.tryOptimisticRead();
        boolean enabled = userEnabled || automaticallyEnabled;
        long usedBytes = retainedFrameBytes.get();
        boolean checkAutomaticMode = !userEnabled && maxDirectMemoryBytes > 0
                && (automaticallyEnabled || inboundRead && automaticEnableThresholdBytes > 0
                        && usedBytes > automaticEnableThresholdBytes);
        boolean checkThrottle = enabled && (usedBytes <= maximumResumeThreshold
                || inboundRead && usedBytes > minimumUnthrottledLimit);
        if (!checkAutomaticMode && !checkThrottle && stateLock.validate(stamp)) {
            return;
        }

        long directMemoryUsedBytes = sampleDirectMemoryUsage(inboundRead);
        AutomaticStateChange automaticStateChange;
        List<Runnable> notifications;
        stamp = stateLock.writeLock();
        try {
            boolean previouslyEnabled = userEnabled || automaticallyEnabled;
            automaticStateChange = updateAutomaticLimitEnabledState(directMemoryUsedBytes, inboundRead);
            // Automatic activation only enables the policy; the next inbound read enforces it. Releases may
            // resume connections, but never start throttling. Read usage inside the write lock, not before it.
            notifications = updateThrottleStates(inboundRead && previouslyEnabled);
        } finally {
            stateLock.unlockWrite(stamp);
        }
        logAutomaticStateChange(automaticStateChange);
        notifyThrottleStateListeners(notifications);
    }

    void setUserEnabled(boolean enabled) {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            userEnabled = enabled;
            if (enabled) {
                automaticallyEnabled = false;
            }
            notifications = updateThrottleStates(enabled);
        } finally {
            stateLock.unlockWrite(stamp);
        }
        notifyThrottleStateListeners(notifications);
    }

    void updateLimitBytes(long limitBytes) {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            if (currentLimiter.getLimitBytes() == limitBytes) {
                return;
            }
            automaticEnableThresholdBytes = automaticEnableThresholdBytes(limitBytes);
            if (limitBytes <= 0) {
                automaticallyEnabled = false;
            }
            // Publish the generation from the listener's value, rather than rereading mutable configuration.
            // Existing connections keep their limit; the automatic activation threshold is broker-wide.
            currentLimiter = new InProgressRequestFrameMemoryLimiter(limitBytes, this);
            notifications = updateThrottleStates(false);
        } finally {
            stateLock.unlockWrite(stamp);
        }
        notifyThrottleStateListeners(notifications);
    }

    void registerThrottleStateListener(InProgressRequestFrameMemoryLimiter limiter, Object key, Runnable listener) {
        List<Runnable> notifications;
        long stamp = stateLock.writeLock();
        try {
            Map<Object, Runnable> listeners = throttleStateListeners.computeIfAbsent(limiter,
                    ignored -> new HashMap<>());
            listeners.put(key, listener);
            if (limiter.updateThrottledState(userEnabled || automaticallyEnabled,
                    retainedFrameBytes.get(), userEnabled)) {
                notifications = new ArrayList<>(listeners.values());
            } else {
                notifications = limiter.isThrottled() ? List.of(listener) : List.of();
            }
            updateThresholdSummaries();
        } finally {
            stateLock.unlockWrite(stamp);
        }
        notifyThrottleStateListeners(notifications);
    }

    void unregisterThrottleStateListener(InProgressRequestFrameMemoryLimiter limiter, Object key) {
        long stamp = stateLock.writeLock();
        try {
            Map<Object, Runnable> listeners = throttleStateListeners.get(limiter);
            if (listeners != null) {
                listeners.remove(key);
                if (listeners.isEmpty()) {
                    // Roots keep the controller alive; closed generations need not remain registered.
                    throttleStateListeners.remove(limiter);
                    updateThresholdSummaries();
                }
            }
        } finally {
            stateLock.unlockWrite(stamp);
        }
    }

    private void updateThresholdSummaries() {
        minimumUnthrottledLimit = Long.MAX_VALUE;
        maximumResumeThreshold = -1;
        for (InProgressRequestFrameMemoryLimiter limiter : throttleStateListeners.keySet()) {
            if (limiter.getLimitBytes() > 0) {
                if (limiter.isThrottled()) {
                    maximumResumeThreshold = Math.max(maximumResumeThreshold, limiter.getLimitBytes() / 2);
                } else {
                    minimumUnthrottledLimit = Math.min(minimumUnthrottledLimit, limiter.getLimitBytes());
                }
            }
        }
    }

    private List<Runnable> updateThrottleStates(boolean allowNewThrottle) {
        List<Runnable> notifications = null;
        long usedBytes = retainedFrameBytes.get();
        boolean enabled = userEnabled || automaticallyEnabled;
        for (var entry : throttleStateListeners.entrySet()) {
            if (entry.getKey().updateThrottledState(enabled, usedBytes, allowNewThrottle)) {
                if (notifications == null) {
                    notifications = new ArrayList<>();
                }
                notifications.addAll(entry.getValue().values());
            }
        }
        updateThresholdSummaries();
        return notifications == null ? List.of() : notifications;
    }

    private long sampleDirectMemoryUsage(boolean allowEnable) {
        long stamp = stateLock.readLock();
        try {
            if (userEnabled || maxDirectMemoryBytes <= 0 || !automaticallyEnabled
                    && (!allowEnable || automaticEnableThresholdBytes <= 0
                            || retainedFrameBytes.get() <= automaticEnableThresholdBytes)) {
                return -1;
            }
        } finally {
            stateLock.unlockRead(stamp);
        }
        return directMemoryUsedBytesSupplier.getAsLong();
    }

    private AutomaticStateChange updateAutomaticLimitEnabledState(long directMemoryUsedBytes, boolean allowEnable) {
        if (userEnabled || directMemoryUsedBytes < 0 || maxDirectMemoryBytes <= 0) {
            return null;
        }
        boolean previouslyEnabled = automaticallyEnabled;
        long usedBytes = retainedFrameBytes.get();
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
