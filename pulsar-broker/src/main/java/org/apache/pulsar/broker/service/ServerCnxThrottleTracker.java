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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Manages and tracks throttling state for server connections in Apache Pulsar.
 *
 * <p>This class provides a centralized mechanism to control connection throttling by managing
 * multiple throttling conditions simultaneously. When throttling is active, it pauses incoming
 * requests by setting Netty's {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)} to
 * {@code false} for the associated channel.
 *
 * <h3>Throttling Mechanism</h3>
 * <p>The tracker maintains independent counters for different types of throttling conditions
 * defined in {@link ThrottleType}. A connection is considered throttled if any of these
 * conditions are active (counter > 0). The connection will only resume normal operation
 * when all throttling conditions are cleared.
 *
 * <h3>Supported Throttling Types</h3>
 * <ul>
 *   <li><b>Connection-level:</b> Max pending publish requests, outbound buffer limits</li>
 *   <li><b>Thread-level:</b> IO thread memory limits for in-flight publishing</li>
 *   <li><b>Topic-level:</b> Topic publish rate limiting</li>
 *   <li><b>Resource Group-level:</b> Resource group publish rate limiting</li>
 *   <li><b>Broker-level:</b> Global broker publish rate limiting</li>
 *   <li><b>Flow Control:</b> Channel writability and cooldown rate limiting</li>
 * </ul>
 *
 * <h3>Reentrant vs Non-Reentrant Types</h3>
 * <p>Some throttling types support multiple concurrent activations (reentrant):
 * <ul>
 *   <li>{@link ThrottleType#TopicPublishRate} - Reentrant because multiple producers may share
 *       the same rate limiter which relates to the same topic</li>
 *   <li>{@link ThrottleType#ResourceGroupPublishRate} - Reentrant because multiple producers may share
 *       the same rate limiter which relates to the same resource group</li>
 * </ul>
 * <p>Other types are non-reentrant and can only be activated once at a time. The reentrant types
 * use counters to track how many producers are affected by the same shared rate limiter, while
 * non-reentrant types use simple boolean states.
 *
 * <h3>Thread Safety</h3>
 * <p>This class is designed to be used from a single thread (the connection's IO thread)
 * and is not thread-safe for concurrent access from multiple threads.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * ServerCnxThrottleTracker tracker = new ServerCnxThrottleTracker(serverCnx);
 *
 * // Mark connection as throttled due to rate limiting
 * tracker.markThrottled(ThrottleType.TopicPublishRate);
 *
 * // Later, when rate limiting condition is cleared
 * tracker.unmarkThrottled(ThrottleType.TopicPublishRate);
 * }</pre>
 *
 * @see ThrottleType
 * @see ThrottleRes
 * @see ServerCnx
 */
@Slf4j
public final class ServerCnxThrottleTracker {

    private final ServerCnx serverCnx;
    private final int[] states = new int[ThrottleType.values().length];

    /**
     * Enumeration of different throttling conditions that can be applied to a server connection.
     *
     * <p>Each type represents a specific resource constraint or rate limiting condition
     * that may require throttling the connection to maintain system stability and fairness.
     *
     * <p>Some types support reentrant behavior (can be activated multiple times concurrently),
     * while others are non-reentrant (single activation only).
     */
    public static enum ThrottleType {

        /**
         * Throttling due to excessive pending publish requests on the connection.
         *
         * <p>This throttling is activated when the number of in-flight publish requests
         * exceeds the configured limit. It helps prevent memory exhaustion and ensures
         * fair resource allocation across connections.
         *
         * <p><b>Type:</b> Non-reentrant
         * <p><b>Configuration:</b> {@link ServiceConfiguration#getMaxPendingPublishRequestsPerConnection()}
         */
        ConnectionMaxQuantityOfInFlightPublishing(false),

        /**
         * Throttling due to excessive memory usage by in-flight publish operations on the IO thread.
         *
         * <p>This throttling is activated when the total memory used by pending publish operations
         * on a shared IO thread exceeds the configured limit. Multiple connections may share the
         * same IO thread, so this limit applies across all connections on that thread.
         *
         * <p><b>Type:</b> Non-reentrant
         * <p><b>Configuration:</b> {@link ServiceConfiguration#getMaxMessagePublishBufferSizeInMB()}
         */
        IOThreadMaxBytesOfInFlightPublishing(false),

        /**
         * Throttling due to topic-level publish rate limiting.
         *
         * <p>This throttling is activated when publish operations exceed the configured
         * rate limits for a specific topic. Multiple producers on the same topic may
         * contribute to triggering this throttling condition.
         *
         * <p><b>Type:</b> Reentrant (supports multiple concurrent activations)
         * <br><b>Reason for reentrancy:</b> Multiple producers may share the same rate limiter
         * which relates to the same topic. Each producer can independently trigger throttling
         * when the shared topic rate limiter becomes active, requiring a counter to track
         * how many producers are affected by the same rate limiter.
         *
         * <p><b>Configuration:</b> Topic-level publish rate policies
         */
        TopicPublishRate(true),

        /**
         * Throttling due to resource group-level publish rate limiting.
         *
         * <p>This throttling is activated when publish operations exceed the configured
         * rate limits for a resource group. Resource groups allow fine-grained control
         * over resource allocation across multiple topics and tenants.
         *
         * <p><b>Type:</b> Reentrant (supports multiple concurrent activations)
         * <br><b>Reason for reentrancy:</b> Multiple producers may share the same rate limiter
         * which relates to the same resource group. Each producer can independently trigger
         * throttling when the shared resource group rate limiter becomes active, requiring
         * a counter to track how many producers are affected by the same rate limiter.
         *
         * <p><b>Configuration:</b> Resource group publish rate policies
         */
        ResourceGroupPublishRate(true),

        /**
         * Throttling due to broker-level publish rate limiting.
         *
         * <p>This throttling is activated when publish operations exceed the global
         * broker-level rate limits. This provides a safety mechanism to prevent
         * the entire broker from being overwhelmed by publish traffic.
         *
         * <p><b>Type:</b> Non-reentrant
         * <p><b>Configuration:</b> {@link ServiceConfiguration#getBrokerPublisherThrottlingMaxMessageRate()}
         * and {@link ServiceConfiguration#getBrokerPublisherThrottlingMaxByteRate()}
         */
        BrokerPublishRate(false),

        /**
         * Throttling due to channel outbound buffer being full.
         *
         * <p>This throttling is activated when the Netty channel's outbound buffer
         * reaches its high water mark, indicating that the client cannot keep up
         * with the rate of outgoing messages. This prevents memory exhaustion
         * and provides backpressure to publishers.
         *
         * <p><b>Type:</b> Non-reentrant
         * <p><b>Reference:</b> PIP-434: Expose Netty channel configuration WRITE_BUFFER_WATER_MARK
         */
        ConnectionOutboundBufferFull(false),

        /**
         * Throttling due to connection pause/resume cooldown rate limiting.
         *
         * <p>This throttling is activated during cooldown periods after a connection
         * has been resumed from a throttled state. It prevents rapid oscillation
         * between throttled and unthrottled states.
         *
         * <p><b>Type:</b> Non-reentrant
         */
        ConnectionPauseReceivingCooldownRateLimit(false);

        @Getter
        final boolean reentrant;

        ThrottleType(boolean reentrant) {
            this.reentrant = reentrant;
        }
    }

    /**
     * Enumeration representing the result of a throttling state change operation.
     *
     * <p>This enum indicates what happened when a throttling condition was marked or unmarked,
     * helping callers understand whether the overall connection state changed or if the
     * operation was ignored.
     */
    enum ThrottleRes {
        /**
         * The operation resulted in a change to the overall connection throttling state.
         *
         * <p>This occurs when:
         * <ul>
         *   <li>The connection transitions from unthrottled to throttled (first throttle type activated)</li>
         *   <li>The connection transitions from throttled to unthrottled (last throttle type deactivated)</li>
         * </ul>
         *
         * <p>When this result is returned, the connection's auto-read setting will be updated
         * accordingly to pause or resume request processing.
         */
        ConnectionStateChanged,

        /**
         * The operation changed the state of the specific throttle type but did not affect
         * the overall connection throttling state.
         *
         * <p>This occurs when:
         * <ul>
         *   <li>A throttle type is activated, but the connection was already throttled by other types</li>
         *   <li>A throttle type is deactivated, but the connection remains throttled by other types</li>
         *   <li>A reentrant throttle type's counter is incremented or decremented</li>
         * </ul>
         */
        TypeStateChanged,

        /**
         * The operation was dropped because it would violate the throttle type's constraints.
         *
         * <p>This occurs when:
         * <ul>
         *   <li>Attempting to mark a non-reentrant throttle type that is already active</li>
         *   <li>Attempting to unmark a throttle type that is not currently active</li>
         *   <li>Attempting to unmark a reentrant throttle type with an invalid counter state</li>
         * </ul>
         */
        Dropped
    }

    /**
     * Checks if the connection is currently throttled by any throttle type.
     *
     * <p>This method examines all throttle type states and returns {@code true}
     * if any of them are active (counter > 0).
     *
     * @return {@code true} if any throttling condition is active, {@code false} otherwise
     */
    private boolean hasThrottled() {
        for (int stat : states) {
            if (stat > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the total count of active throttling conditions across all types.
     *
     * <p>This method sums up all the individual counters for each throttle type,
     * providing a measure of the overall throttling pressure on the connection.
     * For reentrant types, this includes the full counter value (not just 0 or 1).
     *
     * @return the total number of active throttling conditions
     */
    @VisibleForTesting
    public int throttledCount() {
        int i = 0;
        for (int stat : states) {
            i += stat;
        }
        return i;
    }

    /**
     * Marks the connection as throttled for the specified throttle type.
     *
     * <p>This method activates throttling for the given type and may pause the connection's
     * request processing if this is the first active throttling condition. For reentrant
     * types ({@link ThrottleType#TopicPublishRate} and {@link ThrottleType#ResourceGroupPublishRate}),
     * this increments the counter. For non-reentrant types, this sets the state to active.
     *
     * <p>If the connection transitions from unthrottled to throttled, this method will
     * set the Netty channel's auto-read to {@code false}, effectively pausing incoming
     * request processing.
     *
     * <p>Metrics are automatically recorded to track throttling events and connection state changes.
     *
     * @param type the type of throttling condition to activate
     * @throws IllegalArgumentException if type is null
     *
     * @see #unmarkThrottled(ThrottleType)
     * @see ThrottleType
     */
    public void markThrottled(ThrottleType type) {
        assert serverCnx.ctx().executor().inEventLoop() : "This method should be called in serverCnx.ctx().executor()";
        ThrottleRes res = doMarkThrottled(type);
        recordMetricsAfterThrottling(type, res);
        if (res == ThrottleRes.ConnectionStateChanged && isChannelActive()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Setting auto read to false", serverCnx.toString());
            }
            serverCnx.ctx().channel().config().setAutoRead(false);
        }
    }

    /**
     * Unmarks the connection as throttled for the specified throttle type.
     *
     * <p>This method deactivates throttling for the given type and may resume the connection's
     * request processing if this was the last active throttling condition. For reentrant
     * types ({@link ThrottleType#TopicPublishRate} and {@link ThrottleType#ResourceGroupPublishRate}),
     * this decrements the counter. For non-reentrant types, this clears the active state.
     *
     * <p>If the connection transitions from throttled to unthrottled, this method will
     * set the Netty channel's auto-read to {@code true}, effectively resuming incoming
     * request processing.
     *
     * <p>Metrics are automatically recorded to track unthrottling events and connection state changes.
     *
     * @param type the type of throttling condition to deactivate
     * @throws IllegalArgumentException if type is null
     *
     * @see #markThrottled(ThrottleType)
     * @see ThrottleType
     */
    public void unmarkThrottled(ThrottleType type) {
        assert serverCnx.ctx().executor().inEventLoop() : "This method should be called in serverCnx.ctx().executor()";
        ThrottleRes res = doUnmarkThrottled(type);
        recordMetricsAfterUnthrottling(type, res);
        if (res == ThrottleRes.ConnectionStateChanged && isChannelActive()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Setting auto read to true", serverCnx.toString());
            }
            serverCnx.ctx().channel().config().setAutoRead(true);
        }
    }

    /**
     * Internal method to mark a throttle type as active without side effects.
     *
     * <p>This method updates the internal state for the specified throttle type
     * and returns the result of the operation. It handles both reentrant and
     * non-reentrant throttle types appropriately:
     *
     * <ul>
     *   <li><b>Reentrant types:</b> Increment the counter</li>
     *   <li><b>Non-reentrant types:</b> Set to active (1) if not already active</li>
     * </ul>
     *
     * @param throttleType the type of throttling to mark as active
     * @return the result of the marking operation
     * @see ThrottleRes
     */
    private ThrottleRes doMarkThrottled(ThrottleType throttleType) {
        // Two reentrant type: "TopicPublishRate" and "ResourceGroupPublishRate".
        boolean throttled = hasThrottled();
        int value = states[throttleType.ordinal()];
        if (throttleType.isReentrant()) {
            states[throttleType.ordinal()] = value + 1;
        } else {
            states[throttleType.ordinal()] = 1;
            if (value != 0) {
                return ThrottleRes.Dropped;
            }
        }
        return throttled ? ThrottleRes.TypeStateChanged : ThrottleRes.ConnectionStateChanged;
    }

    /**
     * Internal method to unmark a throttle type as active without side effects.
     *
     * <p>This method updates the internal state for the specified throttle type
     * and returns the result of the operation. It handles both reentrant and
     * non-reentrant throttle types appropriately:
     *
     * <ul>
     *   <li><b>Reentrant types:</b> Decrement the counter</li>
     *   <li><b>Non-reentrant types:</b> Clear active state if currently active</li>
     * </ul>
     *
     * @param throttleType the type of throttling to mark as inactive
     * @return the result of the unmarking operation
     * @see ThrottleRes
     */
    private ThrottleRes doUnmarkThrottled(ThrottleType throttleType) {
        int value = states[throttleType.ordinal()];
        if (throttleType.isReentrant()) {
            states[throttleType.ordinal()] = value - 1;
        } else {
            if (value != 1) {
                return ThrottleRes.Dropped;
            }
            states[throttleType.ordinal()] = 0;
        }
        return hasThrottled() ? ThrottleRes.TypeStateChanged : ThrottleRes.ConnectionStateChanged;
    }
    
    /**
     * Records metrics after a throttling operation.
     *
     * <p>This method updates various broker metrics to track throttling events:
     * <ul>
     *   <li>Connection-specific throttling metrics for in-flight publishing limits</li>
     *   <li>Connection pause metrics when the overall connection state changes</li>
     *   <li>Topic-level publish limiting counters</li>
     * </ul>
     *
     * @param type the throttle type that was activated
     * @param res the result of the throttling operation
     */
    private void recordMetricsAfterThrottling(ThrottleType type, ThrottleRes res) {
        if (type == ThrottleType.ConnectionMaxQuantityOfInFlightPublishing && res != ThrottleRes.Dropped) {
            serverCnx.getBrokerService().recordConnectionThrottled();
        }
        if (res == ThrottleRes.ConnectionStateChanged && isChannelActive()) {
            serverCnx.increasePublishLimitedTimesForTopics();
            serverCnx.getBrokerService().recordConnectionPaused();
        }
    }

    /**
     * Records metrics after an unthrottling operation.
     *
     * <p>This method updates various broker metrics to track unthrottling events:
     * <ul>
     *   <li>Connection-specific unthrottling metrics for in-flight publishing limits</li>
     *   <li>Connection resume metrics when the overall connection state changes</li>
     * </ul>
     *
     * @param type the throttle type that was deactivated
     * @param res the result of the unthrottling operation
     */
    private void recordMetricsAfterUnthrottling(ThrottleType type, ThrottleRes res) {
        if (type == ThrottleType.ConnectionMaxQuantityOfInFlightPublishing && res != ThrottleRes.Dropped) {
            serverCnx.getBrokerService().recordConnectionUnthrottled();
        }
        if (res == ThrottleRes.ConnectionStateChanged && isChannelActive()) {
            serverCnx.getBrokerService().recordConnectionResumed();
        }
    }

    public ServerCnxThrottleTracker(ServerCnx serverCnx) {
        this.serverCnx = serverCnx;
    }

    private boolean isChannelActive() {
        return serverCnx.isActive() && serverCnx.ctx() != null && serverCnx.ctx().channel().isActive();
    }
}
