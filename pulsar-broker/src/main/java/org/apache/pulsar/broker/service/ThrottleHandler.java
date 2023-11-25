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
     * Throttles the resource for a specified initial duration, and then continues to throttle it for additional
     * durationsas provided by a supplier until the supplier returns 0.
     *
     * The method works as follows:
     * 1. If the initialPauseTimeNanos is greater than 0, the resource is throttled for this duration.
     * 2. After the initial pause time expires, the method calls the additionalPauseTimeSupplier to get an additional
     * pause time.
     * 3. If the additional pause time is greater than 0, the resource is throttled for this additional duration.
     * 4. Steps 2 and 3 are repeated in a loop until the additionalPauseTimeSupplier returns 0, indicating that no
     * further throttling is required.
     *
     * @param initialPauseTimeNanos       The initial duration (in nanoseconds) for which the resource should be
     *                                    throttled. If this is 0 or less, no initial throttling occurs.
     * @param additionalPauseTimeSupplier A supplier providing additional durations (in nanoseconds) for which the
     *                                    resource should be throttled after the initial pause time. The supplier should
     *                                    return 0 when no further throttling is required.
     */
    void throttle(long initialPauseTimeNanos, LongSupplier additionalPauseTimeSupplier);
}
