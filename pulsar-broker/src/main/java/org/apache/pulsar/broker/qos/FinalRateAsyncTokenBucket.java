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

/**
 * A subclass of {@link AsyncTokenBucket} that represents a token bucket with a rate which is final.
 * The rate and capacity of the token bucket are constant and do not change over time.
 */
class FinalRateAsyncTokenBucket extends AsyncTokenBucket {
    private final long capacity;
    private final long rate;
    private final long ratePeriodNanos;
    private final long targetAmountOfTokensAfterThrottling;

    protected FinalRateAsyncTokenBucket(long capacity, long rate, MonotonicSnapshotClock clockSource,
                                        long ratePeriodNanos, long resolutionNanos, long initialTokens) {
        super(clockSource, resolutionNanos);
        this.capacity = capacity;
        this.rate = rate;
        this.ratePeriodNanos = ratePeriodNanos != -1 ? ratePeriodNanos : ONE_SECOND_NANOS;
        // The target amount of tokens is the amount of tokens made available in the resolution duration
        this.targetAmountOfTokensAfterThrottling = Math.max(this.resolutionNanos * rate / ratePeriodNanos, 1);
        this.tokens = initialTokens;
        tokens(false);
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
