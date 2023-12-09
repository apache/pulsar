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

package org.apache.pulsar.common.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * An asynchronous token bucket algorithm implementation that is optimized for performance with highly concurrent
 * use. CAS (compare-and-swap) operations are used and multiple levels of CAS fields are used to minimize contention
 * when using CAS fields. The {@link LongAdder} class is used in the hot path to hold the sum of consumed tokens.
 * It is eventually consistent, meaning that the tokens are not updated on every call to the "consumeTokens" method.
 * <p>Main usage flow:
 * 1. tokens are consumed by calling the "consumeTokens" method.
 * 2. the "calculateThrottlingDuration" method is called to calculate the duration of a possible needed pause when the
 * tokens
 * are fully consumed.
 * <p>This class does not produce side effects outside of its own scope. It functions similarly to a stateful function,
 * akin to a counter function. In essence, it is a sophisticated counter. It can serve as a foundational component for
 * constructing higher-level asynchronous rate limiter implementations, which require side effects for throttling.
 * <p>To achieve optimal performance, pass a {@link GranularMonotonicClockSource} as the clock source.
 */
public abstract class AsyncTokenBucket {
    public static final MonotonicClockSource DEFAULT_CLOCK_SOURCE = highPrecision -> System.nanoTime();
    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    // 2^24 nanoseconds is 16 milliseconds
    private static final long DEFAULT_RESOLUTION_NANOS = TimeUnit.MILLISECONDS.toNanos(16);

    // The default resolution is 16 milliseconds. This means that the consumed tokens are subtracted from the
    // current amount of tokens about every 16 milliseconds. This solution helps prevent a CAS loop what could cause
    // extra CPU usage when a single CAS field is updated at a high rate from multiple threads.
    private static long defaultResolutionNanos = DEFAULT_RESOLUTION_NANOS;

    // used in tests to disable the optimization and instead use a consistent view of the tokens
    public static void switchToConsistentTokensView() {
        defaultResolutionNanos = 0;
    }

    public static void resetToDefaultEventualConsistentTokensView() {
        defaultResolutionNanos = DEFAULT_RESOLUTION_NANOS;
    }

    // atomic field updaters for the volatile fields in this class

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastNanos");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_INCREMENT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastIncrement");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> TOKENS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "tokens");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> REMAINDER_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "remainderNanos");

    /**
     * This field represents the number of tokens in the bucket. It is eventually consistent, as the
     * pendingConsumedTokens are subtracted from the total number of tokens at most once during each "tick" or
     * "increment", when time advances according to the configured resolution.
     */
    protected volatile long tokens;
    /**
     * This field represents the last time the tokens were updated, in nanoseconds.
     * The configured clockSource is used to obtain the current nanoseconds.
     * By default, a monotonic clock (System.nanoTime()) is used.
     */
    private volatile long lastNanos;
    /**
     * This field represents the last time the tokens were updated, in increments.
     */
    private volatile long lastIncrement;
    /**
     * As time progresses, tokens are added to the bucket. When the rate is low, significant rounding errors could
     * accumulate over time if the remainder nanoseconds are not accounted for in the calculations. This field is used
     * to carry forward the leftover nanoseconds in the update calculation.
     */
    private volatile long remainderNanos;

    /**
     * The resolution in nanoseconds. This is the amount of time that must pass before the tokens are updated.
     */
    protected final long resolutionNanos;
    /**
     * This field is used to obtain the current monotonic clock time in nanoseconds.
     */
    private final MonotonicClockSource clockSource;
    /**
     * This field is used to hold the sum of consumed tokens that are pending to be subtracted from the total amount of
     * tokens. This solution is to prevent CAS loop contention problem. pendingConsumedTokens used JVM's LongAdder
     * which has a complex solution to prevent the CAS loop content problem.
     */
    private final LongAdder pendingConsumedTokens = new LongAdder();

    /**
     * A subclass of {@link AsyncTokenBucket} that represents a token bucket with a rate which is final.
     * The rate and capacity of the token bucket are constant and do not change over time.
     */
    private static class FinalRateAsyncTokenBucket extends AsyncTokenBucket {
        private final long capacity;
        private final long rate;
        private final long ratePeriodNanos;
        private final long targetAmountOfTokensAfterThrottling;

        protected FinalRateAsyncTokenBucket(long capacity, long rate, MonotonicClockSource clockSource,
                                            long ratePeriodNanos, long resolutionNanos, long initialTokens) {
            super(clockSource, resolutionNanos);
            this.capacity = capacity;
            this.rate = rate;
            this.ratePeriodNanos = ratePeriodNanos != -1 ? ratePeriodNanos : ONE_SECOND_NANOS;
            // The target amount of tokens is the amount of tokens made available in the resolution duration
            this.targetAmountOfTokensAfterThrottling = Math.max(this.resolutionNanos * rate / ratePeriodNanos, 1);
            this.tokens = initialTokens;
            updateTokens();
        }

        @Override
        protected final long getRatePeriodNanos() {
            return ratePeriodNanos;
        }

        @Override
        protected final long getTargetAmountOfTokensAfterThrottling() {
            return targetAmountOfTokensAfterThrottling;
        }

        @Override
        public final long getCapacity() {
            return capacity;
        }

        @Override
        public final long getRate() {
            return rate;
        }
    }

    protected AsyncTokenBucket(MonotonicClockSource clockSource, long resolutionNanos) {
        this.clockSource = clockSource;
        this.resolutionNanos = resolutionNanos;
    }

    public static FinalRateAsyncTokenBucketBuilder builder() {
        return new FinalRateAsyncTokenBucketBuilder();
    }

    public static DynamicRateAsyncTokenBucketBuilder builderForDynamicRate() {
        return new DynamicRateAsyncTokenBucketBuilder();
    }

    public void updateTokens() {
        updateAndConsumeTokens(0, false);
    }

    protected abstract long getRatePeriodNanos();

    protected abstract long getTargetAmountOfTokensAfterThrottling();

    private void updateAndConsumeTokens(long consumeTokens, boolean forceUpdateTokens) {
        if (consumeTokens < 0) {
            throw new IllegalArgumentException("consumeTokens must be >= 0");
        }
        long currentNanos = clockSource.getNanos(forceUpdateTokens);
        long currentIncrement = resolutionNanos != 0 ? currentNanos / resolutionNanos : 0;
        long currentLastIncrement = lastIncrement;
        if (forceUpdateTokens || currentIncrement == 0 || (currentIncrement > currentLastIncrement
                && LAST_INCREMENT_UPDATER.compareAndSet(this, currentLastIncrement, currentIncrement))) {
            if (forceUpdateTokens && currentIncrement != 0) {
                LAST_INCREMENT_UPDATER.updateAndGet(this, currentValue -> Math.max(currentValue, currentIncrement));
            }
            long newTokens;
            long previousLastNanos = LAST_NANOS_UPDATER.getAndSet(this, currentNanos);
            if (previousLastNanos == 0) {
                newTokens = 0;
            } else {
                long durationNanos = currentNanos - previousLastNanos + REMAINDER_NANOS_UPDATER.getAndSet(this, 0);
                long currentRate = getRate();
                long currentRatePeriodNanos = getRatePeriodNanos();
                newTokens = (durationNanos * currentRate) / currentRatePeriodNanos;
                long remainderNanos = durationNanos - ((newTokens * currentRatePeriodNanos) / currentRate);
                if (remainderNanos > 0) {
                    REMAINDER_NANOS_UPDATER.addAndGet(this, remainderNanos);
                }
            }
            long pendingConsumed = pendingConsumedTokens.sumThenReset();
            TOKENS_UPDATER.updateAndGet(this,
                    currentTokens -> Math.min(currentTokens + newTokens, getCapacity()) - consumeTokens
                            - pendingConsumed);
        } else {
            if (consumeTokens > 0) {
                pendingConsumedTokens.add(consumeTokens);
            }
        }
    }

    public void consumeTokens(long consumeTokens) {
        updateAndConsumeTokens(consumeTokens, false);
    }

    protected long tokens(boolean forceUpdateTokens) {
        updateAndConsumeTokens(0, forceUpdateTokens);
        return tokens;
    }

    /**
     * Calculate the required throttling duration in nanoseconds to fill up the bucket with the minimum amount of
     * tokens.
     * This method shouldn't be called from the hot path since it calculates a consistent value for the tokens which
     * isn't necessary on the hotpath.
     */
    public long calculateThrottlingDuration() {
        updateAndConsumeTokens(0, true);
        long needTokens = getTargetAmountOfTokensAfterThrottling() - tokens;
        if (needTokens <= 0) {
            return 0;
        }
        return (needTokens * getRatePeriodNanos()) / getRate();
    }

    public abstract long getCapacity();

    public final long getTokens() {
        return tokens(false);
    }

    public abstract long getRate();

    public boolean containsTokens() {
        return containsTokens(false);
    }

    public boolean containsTokens(boolean forceUpdateTokens) {
        return tokens(forceUpdateTokens) > 0;
    }

    // CHECKSTYLE.OFF: ClassTypeParameterName
    public abstract static class AsyncTokenBucketBuilder<SELF extends AsyncTokenBucketBuilder<SELF>> {
        protected MonotonicClockSource clockSource = DEFAULT_CLOCK_SOURCE;
        protected long resolutionNanos = defaultResolutionNanos;

        protected AsyncTokenBucketBuilder() {
        }

        protected SELF self() {
            return (SELF) this;
        }

        public SELF clockSource(MonotonicClockSource clockSource) {
            this.clockSource = clockSource;
            return self();
        }

        public SELF resolutionNanos(long resolutionNanos) {
            this.resolutionNanos = resolutionNanos;
            return self();
        }

        public abstract AsyncTokenBucket build();
    }

    /**
     * A builder class for creating instances of {@link FinalRateAsyncTokenBucket}.
     */
    public static class FinalRateAsyncTokenBucketBuilder
            extends AsyncTokenBucketBuilder<FinalRateAsyncTokenBucketBuilder> {
        protected Long capacity;
        protected Long initialTokens;
        protected Long rate;
        protected long ratePeriodNanos = ONE_SECOND_NANOS;

        protected FinalRateAsyncTokenBucketBuilder() {
        }

        public FinalRateAsyncTokenBucketBuilder rate(long rate) {
            this.rate = rate;
            return this;
        }

        public FinalRateAsyncTokenBucketBuilder ratePeriodNanos(long ratePeriodNanos) {
            this.ratePeriodNanos = ratePeriodNanos;
            return this;
        }

        public FinalRateAsyncTokenBucketBuilder capacity(long capacity) {
            this.capacity = capacity;
            return this;
        }

        public FinalRateAsyncTokenBucketBuilder initialTokens(long initialTokens) {
            this.initialTokens = initialTokens;
            return this;
        }

        public AsyncTokenBucket build() {
            return new FinalRateAsyncTokenBucket(this.capacity != null ? this.capacity : this.rate, this.rate,
                    this.clockSource,
                    this.ratePeriodNanos, this.resolutionNanos,
                    this.initialTokens != null ? this.initialTokens : this.rate
            );
        }
    }

    /**
     * A subclass of {@link AsyncTokenBucket} that represents a token bucket with a dynamic rate.
     * The rate and capacity of the token bucket can change over time based on the rate function and capacity factor.
     */
    public static class DynamicRateAsyncTokenBucket extends AsyncTokenBucket {
        private final LongSupplier rateFunction;
        private final LongSupplier ratePeriodNanosFunction;
        private final double capacityFactor;

        private final double targetFillFactorAfterThrottling;

        protected DynamicRateAsyncTokenBucket(double capacityFactor, LongSupplier rateFunction,
                                              MonotonicClockSource clockSource, LongSupplier ratePeriodNanosFunction,
                                              long resolutionNanos, double initialTokensFactor,
                                              double targetFillFactorAfterThrottling) {
            super(clockSource, resolutionNanos);
            this.capacityFactor = capacityFactor;
            this.rateFunction = rateFunction;
            this.ratePeriodNanosFunction = ratePeriodNanosFunction;
            this.targetFillFactorAfterThrottling = targetFillFactorAfterThrottling;
            this.tokens = (long) (rateFunction.getAsLong() * initialTokensFactor);
            updateTokens();
        }

        @Override
        protected long getRatePeriodNanos() {
            return ratePeriodNanosFunction.getAsLong();
        }

        @Override
        protected long getTargetAmountOfTokensAfterThrottling() {
            return (long) (getRate() * targetFillFactorAfterThrottling);
        }

        @Override
        public long getCapacity() {
            return capacityFactor == 1.0d ? getRate() : (long) (getRate() * capacityFactor);
        }

        @Override
        public long getRate() {
            return rateFunction.getAsLong();
        }
    }

    /**
     * A builder class for creating instances of {@link DynamicRateAsyncTokenBucket}.
     */
    public static class DynamicRateAsyncTokenBucketBuilder
            extends AsyncTokenBucketBuilder<DynamicRateAsyncTokenBucketBuilder> {
        protected LongSupplier rateFunction;
        protected double capacityFactor = 1.0d;
        protected double initialFillFactor = 1.0d;
        protected LongSupplier ratePeriodNanosFunction;
        protected double targetFillFactorAfterThrottling = 0.01d;

        protected DynamicRateAsyncTokenBucketBuilder() {
        }

        public DynamicRateAsyncTokenBucketBuilder rateFunction(LongSupplier rateFunction) {
            this.rateFunction = rateFunction;
            return this;
        }

        public DynamicRateAsyncTokenBucketBuilder ratePeriodNanosFunction(LongSupplier ratePeriodNanosFunction) {
            this.ratePeriodNanosFunction = ratePeriodNanosFunction;
            return this;
        }

        public DynamicRateAsyncTokenBucketBuilder capacityFactor(double capacityFactor) {
            this.capacityFactor = capacityFactor;
            return this;
        }

        public DynamicRateAsyncTokenBucketBuilder initialFillFactor(double initialFillFactor) {
            this.initialFillFactor = initialFillFactor;
            return this;
        }

        public DynamicRateAsyncTokenBucketBuilder targetFillFactorAfterThrottling(
                double targetFillFactorAfterThrottling) {
            this.targetFillFactorAfterThrottling = targetFillFactorAfterThrottling;
            return this;
        }

        @Override
        public AsyncTokenBucket build() {
            return new DynamicRateAsyncTokenBucket(this.capacityFactor, this.rateFunction,
                    this.clockSource,
                    this.ratePeriodNanosFunction, this.resolutionNanos,
                    this.initialFillFactor,
                    targetFillFactorAfterThrottling);
        }
    }

    /**
     * Interface for a clock source that returns a monotonic time in nanoseconds with a required precision.
     */
    public interface MonotonicClockSource {
        /**
         * Returns the current monotonic clock time in nanoseconds.
         *
         * @param highPrecision if true, the returned value must be a high precision monotonic time in nanoseconds.
         *                      if false, the returned value can be a granular precision monotonic time in nanoseconds.
         * @return the current monotonic clock time in nanoseconds
         */
        long getNanos(boolean highPrecision);
    }

    /**
     * A clock source that is optimized for performance by updating the returned low precision monotonic
     * time in a separate thread with a configurable resolution.
     * This resolves a performance bottleneck on platforms where calls to System.nanoTime() are relatively
     * costly. For example, this happens on MacOS with Apple silicon CPUs (M1,M2,M3).
     * Instantiating this class creates a daemon thread that updates the monotonic time. The close method
     * should be called to stop the thread.
     */
    public static class GranularMonotonicClockSource implements MonotonicClockSource, AutoCloseable {
        private final long sleepMillis;
        private final int sleepNanos;
        private final LongSupplier clockSource;
        private volatile long lastNanos;
        private volatile boolean closed;

        public GranularMonotonicClockSource(long granularityNanos, LongSupplier clockSource) {
            this.sleepMillis = TimeUnit.NANOSECONDS.toMillis(granularityNanos);
            this.sleepNanos = (int) (granularityNanos - TimeUnit.MILLISECONDS.toNanos(sleepMillis));
            this.clockSource = clockSource;
            this.lastNanos = clockSource.getAsLong();
            Thread thread = new Thread(this::updateLoop, getClass().getSimpleName() + "-update-loop");
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        public long getNanos(boolean highPrecision) {
            if (highPrecision) {
                long currentNanos = clockSource.getAsLong();
                lastNanos = currentNanos;
                return currentNanos;
            }
            return lastNanos;
        }

        private void updateLoop() {
            while (!closed && !Thread.currentThread().isInterrupted()) {
                lastNanos = clockSource.getAsLong();
                try {
                    Thread.sleep(sleepMillis, sleepNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
