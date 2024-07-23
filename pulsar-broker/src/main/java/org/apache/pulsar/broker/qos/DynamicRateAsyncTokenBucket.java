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

import java.util.function.LongSupplier;

/**
 * A subclass of {@link AsyncTokenBucket} that represents a token bucket with a dynamic rate.
 * The rate and capacity of the token bucket can change over time based on the rate function and capacity factor.
 */
public class DynamicRateAsyncTokenBucket extends AsyncTokenBucket {
    private final LongSupplier rateFunction;
    private final LongSupplier ratePeriodNanosFunction;
    private final double capacityFactor;

    private final double targetFillFactorAfterThrottling;

    protected DynamicRateAsyncTokenBucket(double capacityFactor, LongSupplier rateFunction,
                                          MonotonicSnapshotClock clockSource, LongSupplier ratePeriodNanosFunction,
                                          long resolutionNanos, double initialTokensFactor,
                                          double targetFillFactorAfterThrottling) {
        super(clockSource, resolutionNanos);
        this.capacityFactor = capacityFactor;
        this.rateFunction = rateFunction;
        this.ratePeriodNanosFunction = ratePeriodNanosFunction;
        this.targetFillFactorAfterThrottling = targetFillFactorAfterThrottling;
        this.tokens = (long) (rateFunction.getAsLong() * initialTokensFactor);
        tokens(false);
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
