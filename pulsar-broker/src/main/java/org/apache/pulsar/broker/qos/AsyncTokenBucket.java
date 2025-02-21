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

package org.apache.pulsar.broker.qos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

/**
 * An asynchronous token bucket algorithm implementation that is optimized for performance with highly concurrent
 * use. CAS (compare-and-swap) operations are used and multiple levels of CAS fields are used to minimize contention
 * when using CAS fields. The {@link LongAdder} class is used in the hot path to hold the sum of consumed tokens.
 * It is eventually consistent, meaning that the tokens are not updated on every call to the "consumeTokens" method.
 * <p>Main usage flow:
 * 1. Tokens are consumed by invoking the "consumeTokens" or "consumeTokensAndCheckIfContainsTokens" methods.
 * 2. The "consumeTokensAndCheckIfContainsTokens" or "containsTokens" methods return false if there are no
 * tokens available, indicating a need for throttling.
 * 3. In case of throttling, the application should throttle in a way that is suitable for the use case
 * and then call the "calculateThrottlingDuration" method to calculate the duration of the required pause.
 * 4. After the pause duration, the application should verify if there are any available tokens by invoking the
 * containsTokens method. If tokens are available, the application should cease throttling. However, if tokens are
 * not available, the application should maintain the throttling and recompute the throttling duration. In a
 * concurrent environment, it is advisable to use a throttling queue to ensure fair distribution of resources across
 * throttled connections or clients. Once the throttling duration has elapsed, the application should select the next
 * connection or client from the throttling queue to unthrottle. Before unthrottling, the application should check
 * for available tokens. If tokens are still not available, the application should continue with throttling and
 * repeat the throttling loop.
 * <p>By default, the AsyncTokenBucket is eventually consistent. This means that the token balance is updated
 * with added tokens and consumed tokens at most once during each "increment", when time advances more than the
 * configured resolution. There are settings for configuring consistency, please see {@link AsyncTokenBucketBuilder}
 * for details.
 * <p>This class does not produce side effects outside its own scope. It functions similarly to a stateful function,
 * akin to a counter function. In essence, it is a sophisticated counter. It can serve as a foundational component for
 * constructing higher-level asynchronous rate limiter implementations, which require side effects for throttling.
 * <p>To achieve optimal performance, pass a {@link DefaultMonotonicClock} instance as the clock .
 */
public abstract class AsyncTokenBucket {
    public static final MonotonicClock DEFAULT_SNAPSHOT_CLOCK = new DefaultMonotonicClock();
    static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    // The default add tokens resolution is 16 milliseconds. This means that new tokens are calculated and added
    // to the token balance about every 16 milliseconds. This solution helps prevent a CAS loop what could cause
    // extra CPU usage when a single CAS field is updated at a high rate from multiple threads.
    // 2^24 nanoseconds is 16 milliseconds
    public static final long DEFAULT_ADD_TOKENS_RESOLUTION_NANOS = TimeUnit.MILLISECONDS.toNanos(16);

    // atomic field updaters for the volatile fields in this class

    private static final AtomicLongFieldUpdater<AsyncTokenBucket> LAST_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(AsyncTokenBucket.class, "lastNanos");

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
     * As time progresses, tokens are added to the bucket. When the rate is low, significant rounding errors could
     * accumulate over time if the remainder nanoseconds are not accounted for in the calculations. This field is used
     * to carry forward the leftover nanoseconds in the update calculation.
     */
    private volatile long remainderNanos;
    /**
     * The add tokens resolution in nanoseconds. This is the amount of time that must pass before new tokens are
     * updated.
     */
    protected final long addTokensResolutionNanos;
    /**
     * This field is used to obtain the current monotonic clock time in nanoseconds.
     */
    private final MonotonicClock clockSource;
    /**
     * This field is used to hold the sum of consumed tokens that are pending to be subtracted from the total amount of
     * tokens. This solution is to prevent CAS loop contention problem. pendingConsumedTokens used JVM's LongAdder
     * which has a complex solution to prevent the CAS loop content problem.
     */
    private final LongAdder pendingConsumedTokens = new LongAdder();

    protected AsyncTokenBucket(MonotonicClock clockSource, long addTokensResolutionNanos) {
        this.clockSource = clockSource;
        this.addTokensResolutionNanos = addTokensResolutionNanos;
        this.lastNanos = Long.MIN_VALUE;
    }

    public static FinalRateAsyncTokenBucketBuilder builder() {
        return new FinalRateAsyncTokenBucketBuilder();
    }

    public static DynamicRateAsyncTokenBucketBuilder builderForDynamicRate() {
        return new DynamicRateAsyncTokenBucketBuilder();
    }

    protected abstract long getRatePeriodNanos();

    protected abstract long getTargetAmountOfTokensAfterThrottling();

    /**
     * Consumes tokens and possibly updates the token balance. New tokens are calculated if the last new token
     * calculation occurred more than addTokensResolutionNanos nanoseconds ago. When new tokens are added, the
     * token balance held in the `tokens` field is updated.
     * If the token balance isn't updated, the consumed tokens are added to the pendingConsumedTokens LongAdder
     * counter, which gets flushed the next time the tokens are updated. This design choice optimizes performance
     * by preventing CAS loop contention, which could cause excessive CPU consumption.
     * The returned balance is guaranteed to be off by at most the amount of new tokens gained during
     * addTokensResolutionNanos. In the case of not updating the token balance, the current balance is calculated
     * by subtracting the pendingConsumedTokens sum from the current balance.
     *
     * @param consumeTokens number of tokens to consume; can be 0 to update the token balance
     * @return the current token balance, guaranteed to be off by at most the amount of new tokens gained during
     * addTokensResolutionNanos
     */
    private long consumeTokensAndMaybeUpdateTokensBalance(long consumeTokens) {
        if (consumeTokens < 0) {
            throw new IllegalArgumentException("consumeTokens must be >= 0");
        }
        long currentNanos = clockSource.getTickNanos();
        long newTokens = calculateNewTokensSinceLastUpdate(currentNanos);
        // update token balance if there are new tokens
        if (newTokens > 0) {
            // flush the pendingConsumedTokens by calling "sumThenReset"
            long currentPendingConsumedTokens = pendingConsumedTokens.sumThenReset();
            // calculate the token delta by subtracting the consumed tokens from the new tokens
            long tokenDelta = newTokens - currentPendingConsumedTokens;
            if (tokenDelta != 0 || consumeTokens != 0) {
                // update the tokens and return the current token value
                return TOKENS_UPDATER.updateAndGet(this,
                        // limit the tokens to the capacity of the bucket
                        currentTokens -> Math.min(currentTokens + tokenDelta, getCapacity())
                                // subtract the consumed tokens from the capped tokens
                                - consumeTokens) - pendingConsumedTokens.sum();
            } else {
                return tokens - pendingConsumedTokens.sum();
            }
        } else {
            // tokens are not updated immediately to prevent CAS loop contention

            // add the consumed tokens to the pendingConsumedTokens LongAdder counter
            if (consumeTokens > 0) {
                pendingConsumedTokens.add(consumeTokens);
            }

            // return token balance without updating the balance
            // this might be off by up to the amount of new tokens gained during addTokensResolutionNanos
            return tokens - pendingConsumedTokens.sum();
        }
    }

    /**
     * Calculate the number of new tokens since the last update.
     * This will carry forward the remainder nanos so that a possible rounding error is eliminated.
     *
     * @param currentNanos the current monotonic clock time in nanoseconds
     * @return the number of new tokens to add since the last update
     */
    private long calculateNewTokensSinceLastUpdate(long currentNanos) {
        long previousLastNanos = lastNanos;
        long newLastNanos;
        long minimumIncrementNanos;
        // update lastNanos only if there would be at least one token added
        if (getNanosForOneToken() > addTokensResolutionNanos) {
            minimumIncrementNanos = getNanosForOneToken() - remainderNanos - 1;
        } else {
            minimumIncrementNanos = addTokensResolutionNanos;
        }
        if (currentNanos > previousLastNanos + minimumIncrementNanos) {
            newLastNanos = currentNanos;
        } else {
            newLastNanos = previousLastNanos;
        }
        long newTokens;
        if (newLastNanos == previousLastNanos
                // prevent races with a CAS update of lastNanos
                || !LAST_NANOS_UPDATER.compareAndSet(this, previousLastNanos, newLastNanos)
                || previousLastNanos == Long.MIN_VALUE) {
            newTokens = 0;
        } else {
            long durationNanos = currentNanos - previousLastNanos + REMAINDER_NANOS_UPDATER.getAndSet(this, 0);
            long currentRate = getRate();
            long currentRatePeriodNanos = getRatePeriodNanos();
            // new tokens is the amount of tokens that are created in the duration since the last update
            // with the configured rate
            newTokens = (durationNanos * currentRate) / currentRatePeriodNanos;
            // carry forward the remainder nanos so that the rounding error is eliminated
            long remainderNanos = durationNanos - ((newTokens * currentRatePeriodNanos) / currentRate);
            if (remainderNanos > 0) {
                REMAINDER_NANOS_UPDATER.addAndGet(this, remainderNanos);
            }
        }
        return newTokens;
    }

    /**
     * Eventually consume tokens from the bucket.
     *
     * @param consumeTokens the number of tokens to consume
     */
    public void consumeTokens(long consumeTokens) {
        if (consumeTokens < 0) {
            throw new IllegalArgumentException("consumeTokens must be >= 0");
        }
        if (consumeTokens > 0) {
            pendingConsumedTokens.add(consumeTokens);
        }
    }

    /**
     * Eventually consume tokens from the bucket and check if tokens remain available.
     * The number of tokens is eventually consistent with the configured granularity of resolutionNanos.
     * Therefore, the returned result is not definite.
     *
     * @param consumeTokens the number of tokens to consume
     * @return true if there is tokens remains, false if tokens are all consumed. The answer isn't definite since the
     * comparison is made with eventually consistent token value.
     */
    public boolean consumeTokensAndCheckIfContainsTokens(long consumeTokens) {
        return consumeTokensAndMaybeUpdateTokensBalance(consumeTokens) > 0;
    }

    /**
     * Returns the current token balance. When forceConsistentTokens is true, the tokens balance is updated before
     * returning. If forceConsistentTokens is false, the tokens balance could be updated if the last updated happened
     * more than resolutionNanos nanoseconds ago.
     *
     * @return the current token balance
     */
    private long tokens() {
        return consumeTokensAndMaybeUpdateTokensBalance(0);
    }

    /**
     * Calculate the required throttling duration in nanoseconds to fill up the bucket with the minimum amount of
     * tokens. Will return 0 if there are available tokens in the bucket.
     */
    public long calculateThrottlingDuration() {
        return calculateThrottlingDuration(1);
    }

    /**
     * Calculate the required throttling duration in nanoseconds to fill up the bucket with the required amount
     * of tokens. Will return 0 if the required amount of tokens is already in the bucket.
     */
    public long calculateThrottlingDuration(long requiredTokens) {
        return internalCalculateThrottlingDuration(requiredTokens,
                Math.max(requiredTokens, getTargetAmountOfTokensAfterThrottling()));
    }

    private long internalCalculateThrottlingDuration(long requiredTokens, long minimumTokensForThrottlingCalculation) {
        long currentTokens = consumeTokensAndMaybeUpdateTokensBalance(0);
        if (currentTokens == Long.MIN_VALUE) {
            throw new IllegalArgumentException(
                    "Unexpected result from updateAndConsumeTokens with forceConsistentTokens set to true");
        }
        if (currentTokens >= requiredTokens) {
            return 0L;
        }
        // when currentTokens is negative, subtracting a negative value results in
        // adding the absolute value (-(-x) -> +x)
        long needTokens = minimumTokensForThrottlingCalculation - currentTokens;
        long throttlingDurationNanos = (needTokens * getRatePeriodNanos()) / getRate();
        // calculate when the next token will be added
        long currentNanos = clockSource.getTickNanos();
        long minThrottlingDurations =
                lastNanos + Math.max(getNanosForOneToken(), addTokensResolutionNanos) - currentNanos;
        return Math.max(throttlingDurationNanos, minThrottlingDurations);
    }

    public abstract long getCapacity();

    /**
     * Returns the current number of tokens in the bucket.
     * The token balance is updated if the configured resolutionNanos has passed since the last update unless
     * consistentConsumedTokens is true.
     */
    public final long getTokens() {
        return tokens();
    }

    public abstract long getRate();

    protected abstract long getNanosForOneToken();

    /**
     * Checks if the bucket contains tokens.
     * The token balance is updated before the comparison if the configured resolutionNanos has passed since the last
     * update. It's possible that the returned result is not definite since the token balance is eventually consistent
     * if consistentConsumedTokens is false.
     *
     * @return true if the bucket contains tokens, false otherwise
     */
    public boolean containsTokens() {
        return tokens() > 0;
    }

}
