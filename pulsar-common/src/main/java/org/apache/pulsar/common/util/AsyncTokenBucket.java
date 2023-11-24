package org.apache.pulsar.common.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * An asynchronous token bucket algorithm implementation that is optimized for performance with highly concurrent
 * use. CAS (compare-and-swap) operations are used and multiple levels of CAS fields are used to minimize contention
 * when using CAS fields. The {@link LongAdder} class is used in the hot path to hold the sum of consumed tokens.
 * <p>Main usage flow:
 * 1. tokens are consumed by calling the "consumeTokens" method.
 * 2. the "calculatePauseNanos" method is called to calculate the duration of a possible needed pause when
 * the tokens are fully consumed.
 * <p>This class doesn't have side effects, it's like a stateful function, just like a counter function is a stateful
 * function. Indeed, it is just a sophisticated counter. It can be used as a building block for implementing higher
 * level asynchronous rate limiter implementations which do need side effects.
 */
public class AsyncTokenBucket {

    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_MINIMUM_INCREMENT_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastNanos");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_INCREMENT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastIncrement");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> TOKENS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "tokens");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> REMAINDER_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "remainderNanos");

    // Atomically updated via updaters above
    private volatile long tokens;
    private volatile long lastNanos;
    private volatile long lastIncrement;
    private volatile long remainderNanos;

    private final long capacity;
    private final long rate;
    private final long ratePeriodNanos;
    private final long minIncrementNanos;
    private final LongSupplier clockSource;
    private final LongAdder pendingConsumedTokens = new LongAdder();

    private final long defaultMinTokensForPause;

    public AsyncTokenBucket(long capacity, long rate, LongSupplier clockSource) {
        this(capacity, rate, clockSource, ONE_SECOND_NANOS, DEFAULT_MINIMUM_INCREMENT_NANOS);
    }

    public AsyncTokenBucket(long capacity, long rate, LongSupplier clockSource, long ratePeriodNanos,
                            long minimumIncrementNanos) {
        this.capacity = capacity;
        this.rate = rate;
        this.ratePeriodNanos = ratePeriodNanos;
        this.clockSource = clockSource;
        this.minIncrementNanos =
                Math.max(ratePeriodNanos / rate + 1, minimumIncrementNanos);
        // The default minimum tokens is the amount of tokens made available in the minimum increment duration
        this.defaultMinTokensForPause = this.minIncrementNanos * rate / ratePeriodNanos;
        updateTokens();
    }

    public void updateTokens() {
        updateAndConsumeTokens(0, false);
    }

    private void updateAndConsumeTokens(long consumeTokens, boolean forceUpdateTokens) {
        long currentNanos = clockSource.getAsLong();
        long currentIncrement = currentNanos / minIncrementNanos;
        long currentLastIncrement = lastIncrement;
        if (forceUpdateTokens || (currentIncrement > currentLastIncrement && LAST_INCREMENT_UPDATER
                .compareAndSet(this, currentLastIncrement, currentIncrement))) {
            long newTokens;
            long previousLastNanos = LAST_NANOS_UPDATER.getAndSet(this, currentNanos);
            if (previousLastNanos == 0) {
                newTokens = 0;
            } else {
                long durationNanos = currentNanos - previousLastNanos + REMAINDER_NANOS_UPDATER.getAndSet(this, 0);
                newTokens = (durationNanos * rate) / ratePeriodNanos;
                long remainderNanos = durationNanos - ((newTokens * ratePeriodNanos) / rate);
                if (remainderNanos > 0) {
                    REMAINDER_NANOS_UPDATER.addAndGet(this, remainderNanos);
                }
            }
            TOKENS_UPDATER.updateAndGet(this,
                    currentTokens -> Math.min(currentTokens + newTokens, capacity) - consumeTokens
                            - pendingConsumedTokens.sumThenReset());
        } else {
            if (consumeTokens > 0) {
                pendingConsumedTokens.add(consumeTokens);
            }
        }
    }

    public void consumeTokens(long consumeTokens) {
        updateAndConsumeTokens(consumeTokens, false);
    }

    public long tokens(boolean forceUpdateTokens) {
        if (forceUpdateTokens) {
            updateAndConsumeTokens(0, forceUpdateTokens);
        }
        return tokens;
    }

    private long updateAndConsumeTokensAndCalculatePause(long consumeTokens, long minTokens, boolean forceUpdateTokens) {
        updateAndConsumeTokens(consumeTokens, forceUpdateTokens);
        long needTokens = minTokens - tokens;
        if (needTokens <= 0) {
            return 0;
        }
        return (needTokens * ratePeriodNanos) / rate;
    }

    public long updateAndConsumeTokensAndCalculatePause(long consumeTokens) {
        return updateAndConsumeTokensAndCalculatePause(consumeTokens, defaultMinTokensForPause, false);
    }

    public long getCapacity() {
        return capacity;
    }
}
