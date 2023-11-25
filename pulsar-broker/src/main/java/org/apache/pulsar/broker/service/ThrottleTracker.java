package org.apache.pulsar.broker.service;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class ThrottleTracker {
    public static final long PAUSE_DEDUPLICATION_RESOLUTION = TimeUnit.MILLISECONDS.toNanos(10);
    private static final AtomicIntegerFieldUpdater<ThrottleTracker> THROTTLE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ThrottleTracker.class, "throttleCount");

    private static final AtomicLongFieldUpdater<ThrottleTracker> MAX_END_OF_THROTTLE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(
                    ThrottleTracker.class, "maxEndOfThrottleNanos");
    private volatile int throttleCount;
    private volatile long maxEndOfThrottleNanos = 0L;
    private final Supplier<ChannelHandlerContext> ctxSupplier;
    private final AtomicIntegerFlags throttlingFlags = new AtomicIntegerFlags();

    private final LongSupplier clockSource;


    public ThrottleTracker(Supplier<ChannelHandlerContext> ctxSupplier, LongSupplier clockSource) {
        this.ctxSupplier = ctxSupplier;
        this.clockSource = clockSource;
    }

    public boolean changeThrottlingFlag(int index, boolean throttlingEnabled) {
        if (throttlingFlags.changeFlag(index, throttlingEnabled)) {
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

    public boolean getThrottlingFlag(int index) {
        return throttlingFlags.getFlag(index);
    }

    public void decrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.decrementAndGet(this);
        if (currentThrottleCount == 0) {
            changeAutoRead(true);
        }
    }

    public void incrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.incrementAndGet(this);
        if (currentThrottleCount == 1) {
            changeAutoRead(false);
        }
    }

    protected void changeAutoRead(boolean autoRead) {
        ChannelHandlerContext ctx = ctxSupplier.get();
        if (ctx != null) {
            ctx.channel().config().setAutoRead(autoRead);
        }
    }

    void updatePublishRateLimitersAndMaybeThrottle(List<PublishRateLimiter> rateLimiters, int numOfMessages,
                                                   long msgSizeInBytes) {
        int rateLimitersSize = rateLimiters.size();
        if (rateLimitersSize == 0) {
            return;
        }
        final ThrottleInstruction[] throttleInstructions = new ThrottleInstruction[rateLimitersSize];
        boolean shouldThrottle = false;
        for (int i = 0; i < rateLimitersSize; i++) {
            PublishRateLimiter rateLimiter = rateLimiters.get(i);
            throttleInstructions[i] = rateLimiter.incrementPublishCount(numOfMessages, msgSizeInBytes);
            if (throttleInstructions[i].shouldThrottle()) {
                shouldThrottle = true;
            }
        }
        if (shouldThrottle) {
            long maxPauseTimeNanos = 0;
            for (int i = 0; i < rateLimitersSize; i++) {
                maxPauseTimeNanos = Math.max(maxPauseTimeNanos, throttleInstructions[i].getPauseTimeNanos());
            }
            if (maxPauseTimeNanos > 0) {
                ChannelHandlerContext ctx = ctxSupplier.get();
                if (ctx != null) {
                    long currentMaxEndOfThrottleNanos = maxEndOfThrottleNanos;
                    long endOfThrottleNanos = clockSource.getAsLong() + maxPauseTimeNanos;
                    if (endOfThrottleNanos > currentMaxEndOfThrottleNanos + PAUSE_DEDUPLICATION_RESOLUTION
                            && MAX_END_OF_THROTTLE_UPDATER.compareAndSet(this, currentMaxEndOfThrottleNanos,
                            endOfThrottleNanos)) {
                        incrementThrottleCount();
                        decrementThrottleCountAfterPause(ctx, maxPauseTimeNanos, throttleInstructions);
                    }
                }
            }
        }
    }

    private void decrementThrottleCountAfterPause(ChannelHandlerContext ctx, long pauseTimeNanos,
                                                  ThrottleInstruction[] throttleInstructions) {
        ctx.executor().schedule(() -> {
            long additionalPauseTimeNanos = 0;
            for (int i = 0; i < throttleInstructions.length; i++) {
                additionalPauseTimeNanos = Math.max(additionalPauseTimeNanos,
                        throttleInstructions[i].getAdditionalPauseTimeSupplier().getAsLong());
            }
            if (additionalPauseTimeNanos > 0) {
                long currentMaxEndOfThrottleNanos = maxEndOfThrottleNanos;
                long endOfThrottleNanos = clockSource.getAsLong() + additionalPauseTimeNanos;
                if (endOfThrottleNanos > currentMaxEndOfThrottleNanos + PAUSE_DEDUPLICATION_RESOLUTION
                        && MAX_END_OF_THROTTLE_UPDATER.compareAndSet(this, currentMaxEndOfThrottleNanos,
                        endOfThrottleNanos)) {
                    decrementThrottleCountAfterPause(ctx, additionalPauseTimeNanos, throttleInstructions);
                } else {
                    decrementThrottleCount();
                }
            } else {
                decrementThrottleCount();
            }
        }, pauseTimeNanos, TimeUnit.NANOSECONDS);
    }
}
