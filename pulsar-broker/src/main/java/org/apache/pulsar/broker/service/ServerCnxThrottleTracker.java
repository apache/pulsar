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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks the state of throttling for a connection. The throttling happens by pausing reads by setting
 * Netty {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)} to false for the channel (connection).
 * <p>
 * There can be multiple rate limiters that can throttle a connection. Each rate limiter will independently
 * call the {@link #incrementThrottleCount()} and {@link #decrementThrottleCount()} methods to signal that the
 * connection should be throttled or not. The connection will be throttled if the counter is greater than 0.
 * <p>
 * Besides the rate limiters, the connection can also be throttled if the number of pending publish requests exceeds
 * a configured threshold. This throttling is toggled with the {@link #setPendingSendRequestsExceeded} method.
 * There's also per-thread memory limits which could throttle the connection. This throttling is toggled with the
 * {@link #setPublishBufferLimiting} method. Internally, these two methods will call the
 * {@link #incrementThrottleCount()} and {@link #decrementThrottleCount()} methods when the state changes.
 */
@Slf4j
final class ServerCnxThrottleTracker {

    private static final AtomicIntegerFieldUpdater<ServerCnxThrottleTracker> THROTTLE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ServerCnxThrottleTracker.class, "throttleCount");

    private static final AtomicIntegerFieldUpdater<ServerCnxThrottleTracker>
            PENDING_SEND_REQUESTS_EXCEEDED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ServerCnxThrottleTracker.class, "pendingSendRequestsExceeded");
    private static final AtomicIntegerFieldUpdater<ServerCnxThrottleTracker> PUBLISH_BUFFER_LIMITING_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ServerCnxThrottleTracker.class, "publishBufferLimiting");
    private final ServerCnx serverCnx;
    private volatile int throttleCount;
    private volatile int pendingSendRequestsExceeded;
    private volatile int publishBufferLimiting;


    public ServerCnxThrottleTracker(ServerCnx serverCnx) {
        this.serverCnx = serverCnx;

    }

    /**
     * See {@link Producer#incrementThrottleCount()} for documentation.
     */
    public void incrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.incrementAndGet(this);
        if (currentThrottleCount == 1) {
            changeAutoRead(false);
        }
    }

    /**
     * See {@link Producer#decrementThrottleCount()} for documentation.
     */
    public void decrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.decrementAndGet(this);
        if (currentThrottleCount == 0) {
            changeAutoRead(true);
        }
    }

    private void changeAutoRead(boolean autoRead) {
        if (isChannelActive()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Setting auto read to {}", serverCnx.toString(), autoRead);
            }
            // change the auto read flag on the channel
            serverCnx.ctx().channel().config().setAutoRead(autoRead);
        }
        // update the metrics that track throttling
        if (autoRead) {
            serverCnx.getBrokerService().recordConnectionResumed();
        } else if (isChannelActive()) {
            serverCnx.increasePublishLimitedTimesForTopics();
            serverCnx.getBrokerService().recordConnectionPaused();
        }
    }

    private boolean isChannelActive() {
        return serverCnx.isActive() && serverCnx.ctx() != null && serverCnx.ctx().channel().isActive();
    }

    public void setPublishBufferLimiting(boolean throttlingEnabled) {
        changeThrottlingFlag(PUBLISH_BUFFER_LIMITING_UPDATER, throttlingEnabled);
    }

    public void setPendingSendRequestsExceeded(boolean throttlingEnabled) {
        boolean changed = changeThrottlingFlag(PENDING_SEND_REQUESTS_EXCEEDED_UPDATER, throttlingEnabled);
        if (changed) {
            // update the metrics that track throttling due to pending send requests
            if (throttlingEnabled) {
                serverCnx.getBrokerService().recordConnectionThrottled();
            } else {
                serverCnx.getBrokerService().recordConnectionUnthrottled();
            }
        }
    }

    private boolean changeThrottlingFlag(AtomicIntegerFieldUpdater<ServerCnxThrottleTracker> throttlingFlagFieldUpdater,
                                         boolean throttlingEnabled) {
        // don't change a throttling flag if the channel is not active
        if (!isChannelActive()) {
            return false;
        }
        if (throttlingFlagFieldUpdater.compareAndSet(this, booleanToInt(!throttlingEnabled),
                booleanToInt(throttlingEnabled))) {
            if (throttlingEnabled) {
                incrementThrottleCount();
            } else {
                decrementThrottleCount();
            }
            return true;
        } else {
            return false;
        }
    }

    private static int booleanToInt(boolean value) {
        return value ? 1 : 0;
    }
}
