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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Tracks direct-memory roots retained by inbound request frames and optionally applies backpressure. */
final class InProgressRequestFrameMemoryLimiter {
    private final Object mutex = new Object();
    private final ConcurrentMap<Object, Consumer<Boolean>> throttleStateListeners = new ConcurrentHashMap<>();
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicLong totalUsedBytes;
    private final long limitBytes;
    private final InProgressRequestFrameMemoryLimiterState state;
    private boolean registeredWithState;
    private boolean throttled;

    InProgressRequestFrameMemoryLimiter(long limitBytes, boolean enabled) {
        this(limitBytes, enabled, new AtomicLong());
    }

    InProgressRequestFrameMemoryLimiter(long limitBytes, boolean enabled, AtomicLong totalUsedBytes) {
        this(limitBytes, totalUsedBytes, new InProgressRequestFrameMemoryLimiterState(enabled, totalUsedBytes,
                InProgressRequestFrameMemoryLimiterState.automaticEnableThresholdBytes(limitBytes), limitBytes,
                () -> 0, Long.MAX_VALUE));
    }

    InProgressRequestFrameMemoryLimiter(long limitBytes, AtomicLong totalUsedBytes,
                                        InProgressRequestFrameMemoryLimiterState state) {
        this.limitBytes = limitBytes;
        this.totalUsedBytes = totalUsedBytes;
        this.state = state;
    }

    void registerThrottleStateListener(Object listenerKey, Consumer<Boolean> listener) {
        throttleStateListeners.put(listenerKey, listener);
        registerWithState();
        List<Consumer<Boolean>> throttleStateListenersToNotify;
        boolean throttled;
        synchronized (mutex) {
            boolean previousThrottled = this.throttled;
            throttled = updateThrottledState();
            if (previousThrottled != throttled) {
                throttleStateListenersToNotify = new ArrayList<>(throttleStateListeners.values());
            } else {
                throttleStateListenersToNotify = throttled ? List.of(listener) : null;
            }
        }
        if (throttleStateListenersToNotify != null) {
            notifyThrottleStateListeners(throttleStateListenersToNotify, throttled);
        }
    }

    void unregisterThrottleStateListener(Object listenerKey) {
        throttleStateListeners.remove(listenerKey);
        unregisterFromStateIfUnused();
    }

    void acquire(long bytes) {
        registerWithState();
        updateUsage(bytes);
    }

    void release(long bytes) {
        updateUsage(-bytes);
        if (state.isAutomaticallyEnabled()) {
            // A periodic direct-memory check is unnecessary: this limiter affects only subsequent inbound requests,
            // and releasing an in-progress root buffer is the relevant event that can make those requests runnable.
            state.updateAutomaticLimitEnabledState();
        }
        unregisterFromStateIfUnused();
    }

    private void registerWithState() {
        synchronized (mutex) {
            if (!registeredWithState) {
                state.register(this);
                registeredWithState = true;
            }
        }
    }

    private void unregisterFromStateIfUnused() {
        synchronized (mutex) {
            if (registeredWithState && throttleStateListeners.isEmpty() && usedBytes.get() == 0) {
                state.unregister(this);
                registeredWithState = false;
            }
        }
    }

    private void updateUsage(long delta) {
        usedBytes.addAndGet(delta);
        totalUsedBytes.addAndGet(delta);
        if (state.isAutomaticallyEnabled()) {
            state.updateAutomaticThrottleState();
            return;
        }
        if (!state.isEnabled()) {
            return;
        }
        List<Consumer<Boolean>> throttleStateListenersToNotify;
        boolean throttled;
        synchronized (mutex) {
            if (!state.isEnabled()) {
                return;
            }
            boolean previousThrottled = this.throttled;
            throttled = updateThrottledState();
            throttleStateListenersToNotify = previousThrottled == throttled
                    ? null : new ArrayList<>(throttleStateListeners.values());
        }
        if (throttleStateListenersToNotify != null) {
            notifyThrottleStateListeners(throttleStateListenersToNotify, throttled);
        }
    }

    private boolean updateThrottledState() {
        if (state.isAutomaticallyEnabled()) {
            throttled = state.isAutomaticallyThrottled();
        } else if (!state.isEnabled() || limitBytes <= 0) {
            throttled = false;
        } else if (throttled) {
            throttled = usedBytes.get() > limitBytes / 2;
        } else {
            throttled = usedBytes.get() > limitBytes;
        }
        return throttled;
    }

    private void notifyThrottleStateListeners(List<Consumer<Boolean>> throttleStateListenersToNotify,
                                              boolean throttled) {
        throttleStateListenersToNotify.forEach(listener -> listener.accept(throttled));
    }

    boolean isThrottled() {
        synchronized (mutex) {
            return throttled;
        }
    }

    long usedBytes() {
        return usedBytes.get();
    }

    boolean isEnabled() {
        return state.isEnabled();
    }

    void updateAutomaticLimitEnabledState() {
        state.updateAutomaticLimitEnabledState();
    }

    void onEnabledStateChanged() {
        List<Consumer<Boolean>> throttleStateListenersToNotify;
        boolean throttled;
        synchronized (mutex) {
            boolean previousThrottled = this.throttled;
            throttled = updateThrottledState();
            throttleStateListenersToNotify = previousThrottled == throttled
                    ? null : new ArrayList<>(throttleStateListeners.values());
        }
        if (throttleStateListenersToNotify != null) {
            notifyThrottleStateListeners(throttleStateListenersToNotify, throttled);
        }
    }

}
