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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import lombok.CustomLog;

/** Shared broker-local state for the request-frame memory limiter. */
@CustomLog
final class InProgressRequestFrameMemoryLimiterState {
    private static final int ENABLE_DIRECT_MEMORY_USAGE_PERCENT = 85;
    private static final int DISABLE_DIRECT_MEMORY_USAGE_PERCENT = 65;

    private final Set<InProgressRequestFrameMemoryLimiter> limiters = ConcurrentHashMap.newKeySet();
    private final AtomicLong totalUsedBytes;
    private final LongSupplier directMemoryUsedBytesSupplier;
    private final long maxDirectMemoryBytes;
    private boolean userEnabled;
    private boolean automaticallyEnabled;
    private boolean automaticallyThrottled;
    private long automaticEnableThresholdBytes;

    InProgressRequestFrameMemoryLimiterState(boolean userEnabled, AtomicLong totalUsedBytes,
                                              long automaticEnableThresholdBytes,
                                              LongSupplier directMemoryUsedBytesSupplier, long maxDirectMemoryBytes) {
        this.userEnabled = userEnabled;
        this.totalUsedBytes = totalUsedBytes;
        this.automaticEnableThresholdBytes = automaticEnableThresholdBytes;
        this.directMemoryUsedBytesSupplier = directMemoryUsedBytesSupplier;
        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
    }

    void register(InProgressRequestFrameMemoryLimiter limiter) {
        limiters.add(limiter);
    }

    void unregister(InProgressRequestFrameMemoryLimiter limiter) {
        limiters.remove(limiter);
    }

    synchronized boolean isEnabled() {
        return userEnabled || automaticallyEnabled;
    }

    synchronized boolean isUserEnabled() {
        return userEnabled;
    }

    synchronized boolean isAutomaticallyEnabled() {
        return automaticallyEnabled;
    }

    synchronized boolean isAutomaticallyThrottled() {
        return automaticallyThrottled;
    }

    void setUserEnabled(boolean enabled) {
        boolean stateChanged;
        synchronized (this) {
            stateChanged = userEnabled != enabled || enabled && automaticallyEnabled;
            userEnabled = enabled;
            if (enabled) {
                automaticallyEnabled = false;
                automaticallyThrottled = false;
            }
        }
        if (stateChanged) {
            notifyLimiters();
        }
    }

    void setAutomaticEnableThresholdBytes(long thresholdBytes) {
        boolean notifyLimiters;
        synchronized (this) {
            automaticEnableThresholdBytes = thresholdBytes;
            notifyLimiters = automaticallyEnabled;
            if (automaticallyEnabled && thresholdBytes <= 0) {
                automaticallyEnabled = false;
                automaticallyThrottled = false;
            } else if (automaticallyEnabled) {
                automaticallyThrottled = totalUsedBytes.get() > automaticLimitBytes(thresholdBytes);
            }
        }
        if (notifyLimiters) {
            notifyLimiters();
        }
    }

    void updateAutomaticThrottleState() {
        boolean stateChanged;
        synchronized (this) {
            if (!automaticallyEnabled) {
                return;
            }
            boolean previousThrottled = automaticallyThrottled;
            if (automaticallyThrottled) {
                automaticallyThrottled = totalUsedBytes.get() > automaticEnableThresholdBytes;
            } else {
                automaticallyThrottled = totalUsedBytes.get() > automaticLimitBytes(automaticEnableThresholdBytes);
            }
            stateChanged = previousThrottled != automaticallyThrottled;
        }
        if (stateChanged) {
            notifyLimiters();
        }
    }

    void checkAutoLimit() {
        synchronized (this) {
            if (userEnabled || maxDirectMemoryBytes <= 0) {
                return;
            }
        }
        long directMemoryUsedBytes = directMemoryUsedBytesSupplier.getAsLong();
        boolean stateChanged = false;
        boolean enabled = false;
        long thresholdBytes = 0;
        synchronized (this) {
            if (userEnabled || directMemoryUsedBytes < 0 || maxDirectMemoryBytes <= 0) {
                return;
            }
            if (automaticallyEnabled) {
                if (directMemoryUsedBytes < percentageOf(maxDirectMemoryBytes,
                        DISABLE_DIRECT_MEMORY_USAGE_PERCENT)) {
                    automaticallyEnabled = false;
                    automaticallyThrottled = false;
                    stateChanged = true;
                }
            } else if (directMemoryUsedBytes >= percentageOf(maxDirectMemoryBytes,
                    ENABLE_DIRECT_MEMORY_USAGE_PERCENT)
                    && automaticEnableThresholdBytes > 0
                    && totalUsedBytes.get() > automaticEnableThresholdBytes) {
                automaticallyEnabled = true;
                automaticallyThrottled = totalUsedBytes.get() > automaticLimitBytes(automaticEnableThresholdBytes);
                stateChanged = true;
            }
            enabled = automaticallyEnabled;
            thresholdBytes = automaticEnableThresholdBytes;
        }
        if (stateChanged) {
            log.info().attr("enabled", enabled).attr("directMemoryUsedBytes", directMemoryUsedBytes)
                    .attr("maxDirectMemoryBytes", maxDirectMemoryBytes)
                    .attr("inProgressRequestFrameBytes", totalUsedBytes.get())
                    .attr("inProgressRequestFrameThresholdBytes", thresholdBytes)
                    .log("Automatically updated input-buffer request-frame memory limiting");
            notifyLimiters();
        }
    }

    private void notifyLimiters() {
        limiters.forEach(InProgressRequestFrameMemoryLimiter::onEnabledStateChanged);
    }

    private static long percentageOf(long value, int percentage) {
        return value / 100 * percentage + value % 100 * percentage / 100;
    }

    private static long automaticLimitBytes(long thresholdBytes) {
        return thresholdBytes > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : thresholdBytes * 2;
    }
}
