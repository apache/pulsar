package org.apache.pulsar.broker.service;

import java.util.function.LongSupplier;

/**
 * Interface for throttling a resource.
 * <p>
 * This is used by the Pulsar PublishRateLimiter to throttle a resource that contributes
 * to the publish rate of the broker, a single topic or a resource group.
 */
@FunctionalInterface
public interface ThrottleHandler {
    /**
     * Throttle the resource for the specified time. After the time expires, the handler should
     * call the additionalPauseTimeSupplier to get the additional time to throttle the resource.
     * This should be handled in a loop until additionalPauseTimeSupplier returns 0.
     *
     * @param initialPauseTimeNanos initial time to throttle the resource
     * @param additionalPauseTimeSupplier additional time to throttle the resource
     */
    void throttle(long initialPauseTimeNanos, LongSupplier additionalPauseTimeSupplier);
}
