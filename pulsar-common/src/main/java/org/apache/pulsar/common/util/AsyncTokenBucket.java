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

import com.google.common.annotations.VisibleForTesting;
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
public abstract class AsyncTokenBucket {
    public static final LongSupplier DEFAULT_CLOCK_SOURCE = System::nanoTime;
    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_RESOLUTION_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private static long defaultResolutionNanos = DEFAULT_RESOLUTION_NANOS;

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastNanos");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_INCREMENT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastIncrement");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> TOKENS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "tokens");

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> REMAINDER_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "remainderNanos");

    @VisibleForTesting
    public static void switchToConsistentTokensView() {
        defaultResolutionNanos = 0;
    }

    @VisibleForTesting
    public static void resetToDefaultEventualConsistentTokensView() {
        defaultResolutionNanos = DEFAULT_RESOLUTION_NANOS;
    }

    // Atomically updated via updaters above
    protected volatile long tokens;
    private volatile long lastNanos;
    private volatile long lastIncrement;
    private volatile long remainderNanos;

    private final long minTokens;

    protected final long resolutionNanos;
    private final LongSupplier clockSource;
    private final LongAdder pendingConsumedTokens = new LongAdder();

    private static class FixedRateAsyncTokenBucket extends AsyncTokenBucket {
        private final long capacity;
        private final long rate;
        private final long ratePeriodNanos;
        private final long defaultMinTokensForPause;

        protected FixedRateAsyncTokenBucket(long capacity, long rate, LongSupplier clockSource, long ratePeriodNanos,
                                            long resolutionNanos, long initialTokens, long minTokens) {
            super(clockSource, minTokens, resolutionNanos);
            this.capacity = capacity;
            this.rate = rate;
            this.ratePeriodNanos = ratePeriodNanos != -1 ? ratePeriodNanos : ONE_SECOND_NANOS;
            // The default minimum tokens is the amount of tokens made available in the minimum increment duration
            this.defaultMinTokensForPause = Math.max(this.resolutionNanos * rate / ratePeriodNanos, minTokens);
            this.tokens = initialTokens;
            updateTokens();
        }

        @Override
        protected final long getRatePeriodNanos() {
            return ratePeriodNanos;
        }

        @Override
        protected final long getMinTokensForPause() {
            return defaultMinTokensForPause;
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

    protected AsyncTokenBucket(LongSupplier clockSource, long minTokens, long resolutionNanos) {
        this.clockSource = clockSource;
        this.minTokens = minTokens;
        this.resolutionNanos = resolutionNanos;
    }

    public static FixedRateAsyncTokenBucketBuilder builder() {
        return new FixedRateAsyncTokenBucketBuilder();
    }

    public static DynamicRateAsyncTokenBucketBuilder builderForDynamicRate() {
        return new DynamicRateAsyncTokenBucketBuilder();
    }

    public void updateTokens() {
        updateAndConsumeTokens(0, false);
    }

    protected abstract long getRatePeriodNanos();

    protected abstract long getMinTokensForPause();

    private void updateAndConsumeTokens(long consumeTokens, boolean forceUpdateTokens) {
        if (consumeTokens < 0) {
            throw new IllegalArgumentException("consumeTokens must be >= 0");
        }
        long currentNanos = clockSource.getAsLong();
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

    protected long updateAndConsumeTokensAndCalculatePause(long consumeTokens, long minTokens,
                                                           boolean forceUpdateTokens) {
        updateAndConsumeTokens(consumeTokens, forceUpdateTokens);
        long needTokens = minTokens - tokens;
        if (needTokens <= 0) {
            return 0;
        }
        return (needTokens * getRatePeriodNanos()) / getRate();
    }

    /**
     * Calculate the pause duration in nanoseconds if the tokens are fully consumed.
     * This method shouldn't be called from the hot path since it calculates a consistent value for the tokens which
     * isn't necessary on the hotpath.
     *
     * @param forceUpdateTokens
     */
    public long calculatePause(boolean forceUpdateTokens) {
        return updateAndConsumeTokensAndCalculatePause(0, getMinTokensForPause(), forceUpdateTokens);
    }

    public abstract long getCapacity();

    public final long getTokens() {
        return tokens(false);
    }

    public abstract long getRate();

    // TODO: LH Rename to something that is a better name
    public boolean containsTokens() {
        return containsTokens(false);
    }

    public boolean containsTokens(boolean forceUpdateTokens) {
        return tokens(forceUpdateTokens) >= minTokens;
    }

    // CHECKSTYLE.OFF: ClassTypeParameterName
    public abstract static class AsyncTokenBucketBuilder<SELF extends AsyncTokenBucketBuilder<SELF>> {
        protected LongSupplier clockSource = DEFAULT_CLOCK_SOURCE;
        protected long minTokens = 1L;
        protected long resolutionNanos = defaultResolutionNanos;

        protected AsyncTokenBucketBuilder() {
        }

        protected SELF self() {
            return (SELF) this;
        }

        public SELF clockSource(LongSupplier clockSource) {
            this.clockSource = clockSource;
            return self();
        }

        public SELF minTokens(long minTokens) {
            this.minTokens = minTokens;
            return self();
        }

        public SELF resolutionNanos(long resolutionNanos) {
            this.resolutionNanos = resolutionNanos;
            return self();
        }

        public abstract AsyncTokenBucket build();
    }

    public static class FixedRateAsyncTokenBucketBuilder
            extends AsyncTokenBucketBuilder<FixedRateAsyncTokenBucketBuilder> {
        protected Long capacity;
        protected Long initialTokens;
        protected Long rate;
        protected long ratePeriodNanos = ONE_SECOND_NANOS;

        protected FixedRateAsyncTokenBucketBuilder() {
        }

        public FixedRateAsyncTokenBucketBuilder rate(long rate) {
            this.rate = rate;
            return this;
        }

        public FixedRateAsyncTokenBucketBuilder ratePeriodNanos(long ratePeriodNanos) {
            this.ratePeriodNanos = ratePeriodNanos;
            return this;
        }

        public FixedRateAsyncTokenBucketBuilder capacity(long capacity) {
            this.capacity = capacity;
            return this;
        }

        public FixedRateAsyncTokenBucketBuilder initialTokens(long initialTokens) {
            this.initialTokens = initialTokens;
            return this;
        }

        public AsyncTokenBucket build() {
            return new FixedRateAsyncTokenBucket(this.capacity != null ? this.capacity : this.rate, this.rate,
                    this.clockSource,
                    this.ratePeriodNanos, this.resolutionNanos,
                    this.initialTokens != null ? this.initialTokens : this.rate,
                    this.minTokens);
        }
    }

    public static class DynamicRateAsyncTokenBucketBuilder
            extends AsyncTokenBucketBuilder<DynamicRateAsyncTokenBucketBuilder> {
        protected LongSupplier rateFunction;
        protected double capacityFactor = 1.0d;
        protected double initialTokensFactor = 1.0d;
        protected LongSupplier ratePeriodNanosFunction;
        protected double minTokensForPauseFactor = 0.01d;

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

        public DynamicRateAsyncTokenBucketBuilder initialTokensFactor(double initialTokensFactor) {
            this.initialTokensFactor = initialTokensFactor;
            return this;
        }

        public DynamicRateAsyncTokenBucketBuilder minTokensForPauseFactor(double minTokensForPauseFactor) {
            this.minTokensForPauseFactor = minTokensForPauseFactor;
            return this;
        }

        @Override
        public AsyncTokenBucket build() {
            return new DynamicRateAsyncTokenBucket(this.capacityFactor, this.rateFunction,
                    this.clockSource,
                    this.ratePeriodNanosFunction, this.resolutionNanos,
                    this.initialTokensFactor,
                    this.minTokens, minTokensForPauseFactor);
        }

    }

    public static class DynamicRateAsyncTokenBucket extends AsyncTokenBucket {
        private final LongSupplier rateFunction;
        private final LongSupplier ratePeriodNanosFunction;
        private final double capacityFactor;

        private final double minTokensForPauseFactor;

        protected DynamicRateAsyncTokenBucket(double capacityFactor, LongSupplier rateFunction,
                                              LongSupplier clockSource, LongSupplier ratePeriodNanosFunction,
                                              long resolutionNanos, double initialTokensFactor, long minTokens,
                                              double minTokensForPauseFactor) {
            super(clockSource, minTokens, resolutionNanos);
            this.capacityFactor = capacityFactor;
            this.rateFunction = rateFunction;
            this.ratePeriodNanosFunction = ratePeriodNanosFunction;
            this.minTokensForPauseFactor = minTokensForPauseFactor;
            this.tokens = (long) (rateFunction.getAsLong() * initialTokensFactor);
            updateTokens();
        }

        @Override
        protected long getRatePeriodNanos() {
            return ratePeriodNanosFunction.getAsLong();
        }

        @Override
        protected long getMinTokensForPause() {
            return (long) (getRate() * minTokensForPauseFactor);
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
}
