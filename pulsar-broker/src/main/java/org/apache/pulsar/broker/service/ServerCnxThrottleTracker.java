package org.apache.pulsar.broker.service;

import io.prometheus.client.Gauge;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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
 * {@link #setPublishBufferLimiting} method. Internally, these two methods will call the {@link #incrementThrottleCount()}
 * and {@link #decrementThrottleCount()} methods.
 */
final class ServerCnxThrottleTracker {
    private static final Gauge throttledConnections = Gauge.build()
            .name("pulsar_broker_throttled_connections")
            .help("Counter of connections throttled because of per-connection limit")
            .register();

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
     * Increments the counter that controls the throttling of the connection by pausing reads.
     * The connection will be throttled while the counter is greater than 0.
     * <p>
     * The caller is responsible for decrementing the counter by calling {@link #decrementThrottleCount()}  when the
     * connection should no longer be throttled.
     */
    public void incrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.incrementAndGet(this);
        if (currentThrottleCount == 1) {
            changeAutoRead(false);
        }
    }

    /**
     * Decrements the counter that controls the throttling of the connection by pausing reads.
     * The connection will be throttled while the counter is greater than 0.
     * <p>
     * This method should be called when the connection should no longer be throttled. However, the caller should have
     * previously called {@link #incrementThrottleCount()}.
     */
    public void decrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.decrementAndGet(this);
        if (currentThrottleCount == 0) {
            changeAutoRead(true);
        }
    }

    private void changeAutoRead(boolean autoRead) {
        serverCnx.ctx().channel().config().setAutoRead(autoRead);
        if (autoRead) {
            serverCnx.getBrokerService().resumedConnections(1);
        } else {
            serverCnx.increasePublishLimitedTimesForTopics();
            serverCnx.getBrokerService().pausedConnections(1);
        }
    }

    public void setPublishBufferLimiting(boolean throttlingEnabled) {
        changeThrottlingFlag(PUBLISH_BUFFER_LIMITING_UPDATER, throttlingEnabled);
    }

    public void setPendingSendRequestsExceeded(boolean throttlingEnabled) {
        boolean changed = changeThrottlingFlag(PENDING_SEND_REQUESTS_EXCEEDED_UPDATER, throttlingEnabled);
        if (changed) {
            if (throttlingEnabled) {
                throttledConnections.inc();
            } else {
                throttledConnections.dec();
            }
        }
    }

    private boolean changeThrottlingFlag(AtomicIntegerFieldUpdater<ServerCnxThrottleTracker> throttlingFlagFieldUpdater,
                                         boolean throttlingEnabled) {
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
