package org.apache.pulsar.broker.service;

import java.util.function.LongSupplier;
import lombok.Value;

@Value
public class ThrottleInstruction {
    public static final ThrottleInstruction NO_THROTTLE = new ThrottleInstruction(0, () -> 0L);
    private final long pauseTimeNanos;
    private final LongSupplier additionalPauseTimeSupplier;

    public boolean shouldThrottle() {
        return pauseTimeNanos > 0;
    }
}
