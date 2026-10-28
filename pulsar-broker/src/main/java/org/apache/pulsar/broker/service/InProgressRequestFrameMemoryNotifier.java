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

import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;

/** Coalesces throttle changes into one pending task per event loop, across all configuration generations. */
@CustomLog
final class InProgressRequestFrameMemoryNotifier implements Runnable {
    private final InProgressRequestFrameMemoryController controller;
    private final EventExecutor executor;
    private final AtomicInteger pendingSignals = new AtomicInteger();
    // Only the owning event loop accesses connection callbacks. The controller registers one signal callback
    // per generation on this loop, so it neither copies every connection nor schedules a task for each one.
    private final Map<InProgressRequestFrameMemoryLimiter, Map<Object, Runnable>> listeners = new HashMap<>();

    InProgressRequestFrameMemoryNotifier(InProgressRequestFrameMemoryController controller, EventExecutor executor) {
        this.controller = controller;
        this.executor = executor;
    }

    void add(InProgressRequestFrameMemoryLimiter limiter, Object key, Runnable listener) {
        Map<Object, Runnable> connections = listeners.get(limiter);
        if (connections == null) {
            connections = new HashMap<>();
            listeners.put(limiter, connections);
            limiter.registerThrottleStateListener(this, this::signal);
        }
        connections.put(key, listener);
        if (limiter.isThrottled()) {
            signal();
        }
    }

    void remove(InProgressRequestFrameMemoryLimiter limiter, Object key) {
        Map<Object, Runnable> connections = listeners.get(limiter);
        if (connections != null) {
            connections.remove(key);
            if (connections.isEmpty()) {
                listeners.remove(limiter);
                limiter.unregisterThrottleStateListener(this);
            }
        }
        if (listeners.isEmpty()) {
            controller.removeEventLoopNotifier(executor, this);
        }
    }

    private void signal() {
        if (pendingSignals.getAndIncrement() == 0) {
            try {
                // Always defer, including signals from this event loop: finish delivering the current input
                // before applying backpressure to subsequent reads.
                executor.execute(this);
            } catch (RejectedExecutionException ignored) {
                // The event loop is shutting down; its connections no longer need a throttle update.
                pendingSignals.set(0);
            }
        }
    }

    @Override
    public void run() {
        int signals = pendingSignals.get();
        do {
            // A callback can synchronously remove a channel or register another one. Snapshot on the event loop,
            // outside the controller lock, and let each callback read the limiter's latest state when executed.
            ArrayList<Runnable> callbacks = new ArrayList<>();
            listeners.values().forEach(connections -> callbacks.addAll(connections.values()));
            for (Runnable callback : callbacks) {
                try {
                    callback.run();
                } catch (RuntimeException error) {
                    log.warn().exception(error).log("Failed to apply request-frame throttling to a connection");
                }
            }
            // A signal arriving during delivery or at drain completion must either be consumed here or schedule
            // a new task. Merely clearing a boolean after delivery can lose the last release's recovery signal.
            signals = pendingSignals.addAndGet(-signals);
        } while (signals != 0);
    }
}
