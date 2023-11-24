package org.apache.pulsar.broker.service;

import io.netty.channel.ChannelHandlerContext;
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

    public ThrottleTracker(Supplier<ChannelHandlerContext> ctxSupplier) {
        this.ctxSupplier = ctxSupplier;
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

    public void throttleForNanos(long nanos) {
        ChannelHandlerContext ctx = ctxSupplier.get();
        if (ctx != null) {
            incrementThrottleCount();
            ctx.executor().schedule(this::decrementThrottleCount, nanos, TimeUnit.NANOSECONDS);
        }
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
}
