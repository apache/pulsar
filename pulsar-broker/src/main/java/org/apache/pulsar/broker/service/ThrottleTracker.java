package org.apache.pulsar.broker.service;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

public class ThrottleTracker {
    private final Supplier<ChannelHandlerContext> ctxSupplier;
    private final AtomicIntegerFlags flags = new AtomicIntegerFlags();
    private static final AtomicIntegerFieldUpdater<ThrottleTracker> THROTTLE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ThrottleTracker.class, "throttleCount");
    private volatile int throttleCount;

    public ThrottleTracker(Supplier<ChannelHandlerContext> ctxSupplier) {
        this.ctxSupplier = ctxSupplier;
    }

    public boolean changeFlag(int index, boolean enabled) {
        if (flags.changeFlag(index, enabled)) {
            if (enabled) {
                incrementThrottleCount();
            } else {
                decrementThrottleCount();
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean getFlag(int index) {
        return flags.getFlag(index);
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
