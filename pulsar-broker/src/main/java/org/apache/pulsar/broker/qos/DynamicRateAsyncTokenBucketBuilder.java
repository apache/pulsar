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
 * A builder class for creating instances of {@link DynamicRateAsyncTokenBucket}.
 */
public class DynamicRateAsyncTokenBucketBuilder
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
                this.clock,
                this.ratePeriodNanosFunction, this.resolutionNanos,
                this.initialFillFactor,
                targetFillFactorAfterThrottling);
    }
}
