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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import lombok.CustomLog;

/**
 * Accounts for all retained inbound roots in a broker and controls its request-frame limiters.
 * Each configuration generation has an immutable limit, but all generations use the same memory counter.
 * Policy and listener registration are serialized on this controller; callbacks always run outside its monitor.
 */
@CustomLog
final class InProgressRequestFrameMemoryController {
    private static final int ENABLE_DIRECT_MEMORY_USAGE_PERCENT = 85;
    private static final int DISABLE_DIRECT_MEMORY_USAGE_PERCENT = 65;

    private final AtomicLong retainedFrameBytes = new AtomicLong();
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
    synchronized int getRegisteredLimiterCount() {
        return throttleStateListeners.size();
    }

    synchronized boolean isEnabled() {
        return userEnabled || automaticallyEnabled;
    }

    synchronized boolean isAutomaticallyEnabled() {
        return automaticallyEnabled;
    }

    void acquire(long bytes) {
        retainedFrameBytes.addAndGet(bytes);
    }

    void release(long bytes) {
        retainedFrameBytes.addAndGet(-bytes);
        long directMemoryUsedBytes = isAutomaticallyEnabled() ? sampleDirectMemoryUsage() : -1;
        AutomaticStateChange automaticStateChange;
        List<Runnable> notifications;
        synchronized (this) {
            automaticStateChange = updateAutomaticLimitEnabledState(directMemoryUsedBytes, false);
            // Release may resume paused connections, but must not start throttling before another inbound read.
            notifications = updateThrottleStates(false);
        }
        logAutomaticStateChange(automaticStateChange);
        notifyThrottleStateListeners(notifications);
        // No periodic task is needed: new throttling only affects subsequent inbound reads. Root releases must
        // still trigger recovery, since a paused connection cannot supply the next inbound event itself.
    }

    void onInboundRead() {
        long directMemoryUsedBytes = sampleDirectMemoryUsage();
        AutomaticStateChange automaticStateChange;
        List<Runnable> notifications;
        synchronized (this) {
            boolean previouslyEnabled = isEnabled();
            automaticStateChange = updateAutomaticLimitEnabledState(directMemoryUsedBytes, true);
            // An automatic false -> true transition only enables the policy. The next inbound read enforces it,
            // including reads which reuse an existing cumulation without allocating another root.
            notifications = updateThrottleStates(previouslyEnabled);
        }
        logAutomaticStateChange(automaticStateChange);
        notifyThrottleStateListeners(notifications);
    }

    void setUserEnabled(boolean enabled) {
        List<Runnable> notifications;
        synchronized (this) {
            userEnabled = enabled;
            if (enabled) {
                automaticallyEnabled = false;
            }
            notifications = updateThrottleStates(enabled);
        }
        notifyThrottleStateListeners(notifications);
    }

    void updateLimitBytes(long limitBytes) {
        List<Runnable> notifications;
        synchronized (this) {
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
        }
        notifyThrottleStateListeners(notifications);
    }

    void registerThrottleStateListener(InProgressRequestFrameMemoryLimiter limiter, Object key, Runnable listener) {
        List<Runnable> notifications;
        synchronized (this) {
            Map<Object, Runnable> listeners = throttleStateListeners.computeIfAbsent(limiter,
                    ignored -> new HashMap<>());
            listeners.put(key, listener);
            if (limiter.updateThrottledState(isEnabled(), retainedFrameBytes.get(), userEnabled)) {
                notifications = new ArrayList<>(listeners.values());
            } else {
                notifications = limiter.isThrottled() ? List.of(listener) : List.of();
            }
        }
        notifyThrottleStateListeners(notifications);
    }

    synchronized void unregisterThrottleStateListener(InProgressRequestFrameMemoryLimiter limiter, Object key) {
        Map<Object, Runnable> listeners = throttleStateListeners.get(limiter);
        if (listeners != null) {
            listeners.remove(key);
            if (listeners.isEmpty()) {
                // Roots retain this controller, so later release need not keep a closed generation alive.
                throttleStateListeners.remove(limiter);
            }
        }
    }

    private List<Runnable> updateThrottleStates(boolean allowNewThrottle) {
        List<Runnable> notifications = null;
        long usedBytes = retainedFrameBytes.get();
        boolean enabled = isEnabled();
        for (var entry : throttleStateListeners.entrySet()) {
            if (entry.getKey().updateThrottledState(enabled, usedBytes, allowNewThrottle)) {
                if (notifications == null) {
                    notifications = new ArrayList<>();
                }
                notifications.addAll(entry.getValue().values());
            }
        }
        return notifications == null ? List.of() : notifications;
    }

    private long sampleDirectMemoryUsage() {
        synchronized (this) {
            if (userEnabled || maxDirectMemoryBytes <= 0) {
                return -1;
            }
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
