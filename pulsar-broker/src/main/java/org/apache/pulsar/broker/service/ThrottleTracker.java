package org.apache.pulsar.broker.service;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

public class ThrottleTracker {
    private final Supplier<ChannelHandlerContext> ctxSupplier;
    private static final AtomicIntegerFieldUpdater<ThrottleTracker> THROTTLE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ThrottleTracker.class, "throttleCount");
    private volatile int throttleCount;
    private final AtomicIntegerFlags throttlingFlags = new AtomicIntegerFlags();

    private final List<PublishRateLimiter> rateLimiters;
    private int rateLimitersSize;

    public ThrottleTracker(Supplier<ChannelHandlerContext> ctxSupplier, List<PublishRateLimiter> rateLimiters) {
        this.ctxSupplier = ctxSupplier;
        this.rateLimiters = List.copyOf(rateLimiters);
        this.rateLimitersSize = rateLimiters.size();
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

    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
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
                    incrementThrottleCount();
                    decrementThrottleCountAfterPause(ctx, maxPauseTimeNanos, throttleInstructions);
                }
            }
        }
    }

    private void decrementThrottleCountAfterPause(ChannelHandlerContext ctx, long pauseTimeNanos,
                                                  ThrottleInstruction[] throttleInstructions) {
        ctx.executor().schedule(() -> {
            long additionalPauseTimeNanos = 0;
            for (int i = 0; i < rateLimitersSize; i++) {
                additionalPauseTimeNanos = Math.max(additionalPauseTimeNanos,
                        throttleInstructions[i].getAdditionalPauseTimeSupplier().getAsLong());
            }
            if (additionalPauseTimeNanos > 0) {
                decrementThrottleCountAfterPause(ctx, additionalPauseTimeNanos, throttleInstructions);
            } else {
                decrementThrottleCount();
            }
        }, pauseTimeNanos, TimeUnit.NANOSECONDS);
    }
}
