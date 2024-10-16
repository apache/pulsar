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
 * A builder class for creating instances of {@link FinalRateAsyncTokenBucket}.
 */
public class FinalRateAsyncTokenBucketBuilder
        extends AsyncTokenBucketBuilder<FinalRateAsyncTokenBucketBuilder> {
    protected Long capacity;
    protected Long initialTokens;
    protected Long rate;
    protected long ratePeriodNanos = AsyncTokenBucket.ONE_SECOND_NANOS;

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
                this.clock,
                this.ratePeriodNanos, this.resolutionNanos,
                this.initialTokens != null ? this.initialTokens : this.rate
        );
    }
}
