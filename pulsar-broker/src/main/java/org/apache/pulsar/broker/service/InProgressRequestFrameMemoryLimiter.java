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

/**
 * An immutable limit shared by connections created in the same configuration generation.
 * Manual and automatic modes compare broker-wide retained frame memory against this generation's limit.
 */
final class InProgressRequestFrameMemoryLimiter {
    private final InProgressRequestFrameMemoryController controller;
    private final long limitBytes;
    private volatile boolean throttled;

    InProgressRequestFrameMemoryLimiter(long limitBytes, InProgressRequestFrameMemoryController controller) {
        this.limitBytes = limitBytes;
        this.controller = controller;
    }

    InProgressRequestFrameMemoryController getController() {
        return controller;
    }

    long getLimitBytes() {
        return limitBytes;
    }

    void registerThrottleStateListener(Object listenerKey, Runnable listener) {
        controller.registerThrottleStateListener(this, listenerKey, listener);
    }

    void unregisterThrottleStateListener(Object listenerKey) {
        controller.unregisterThrottleStateListener(this, listenerKey);
    }

    // Called only while holding the controller write lock. Release events may recover, but cannot start throttling.
    boolean updateThrottledState(boolean enabled, long usedBytes, boolean allowNewThrottle) {
        boolean previouslyThrottled = throttled;
        if (!enabled || limitBytes <= 0) {
            throttled = false;
        } else if (throttled) {
            throttled = usedBytes > limitBytes / 2;
        } else if (allowNewThrottle) {
            throttled = usedBytes > limitBytes;
        }
        return previouslyThrottled != throttled;
    }

    boolean isThrottled() {
        return throttled;
    }
}
