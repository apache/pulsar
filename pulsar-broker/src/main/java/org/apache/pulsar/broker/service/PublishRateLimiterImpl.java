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

package org.apache.pulsar.broker.service;

import java.util.function.LongSupplier;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.AsyncTokenBucket;

public class PublishRateLimiterImpl implements PublishRateLimiter {
    private static final int BURST_FACTOR = 2;
    private volatile AsyncTokenBucket tokenBucketOnMessage;
    private volatile AsyncTokenBucket tokenBucketOnByte;
    private final LongSupplier clockSource;

    public PublishRateLimiterImpl(Policies policies, String clusterName) {
        this();
        update(policies, clusterName);
    }

    public PublishRateLimiterImpl(PublishRate maxPublishRate) {
        this();
        update(maxPublishRate);
    }

    public PublishRateLimiterImpl() {
        this.clockSource = AsyncTokenBucket.DEFAULT_CLOCK_SOURCE;
    }

    public PublishRateLimiterImpl(LongSupplier clockSource) {
        this.clockSource = clockSource;
    }

    @Override
    public ThrottleInstruction consumePublishQuota(int numOfMessages, long msgSizeInBytes) {
        long pauseNanos = 0L;
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        if (currentTokenBucketOnMessage != null) {
            pauseNanos = currentTokenBucketOnMessage.updateAndConsumeTokensAndCalculatePause(numOfMessages);
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            pauseNanos = Math.max(pauseNanos,
                    currentTokenBucketOnByte.updateAndConsumeTokensAndCalculatePause(msgSizeInBytes));
        }
        if (pauseNanos > 0) {
            return new ThrottleInstruction(pauseNanos, this::calculateAdditionalPause);
        } else {
            return ThrottleInstruction.NO_THROTTLE;
        }
    }

    private long calculateAdditionalPause() {
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        long pauseNanos = 0L;
        if (currentTokenBucketOnMessage != null) {
            pauseNanos = currentTokenBucketOnMessage.calculatePause();
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            pauseNanos = Math.max(pauseNanos,
                    currentTokenBucketOnByte.calculatePause());
        }
        return pauseNanos;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        update(maxPublishRate);
    }

    public void update(PublishRate maxPublishRate) {
        if (maxPublishRate != null) {
            updateTokenBuckets(maxPublishRate.publishThrottlingRateInMsg, maxPublishRate.publishThrottlingRateInByte);
        } else {
            tokenBucketOnMessage = null;
            tokenBucketOnByte = null;
        }
    }

    protected void updateTokenBuckets(long publishThrottlingRateInMsg, long publishThrottlingRateInByte) {
        if (publishThrottlingRateInMsg > 0) {
            tokenBucketOnMessage = new AsyncTokenBucket(BURST_FACTOR * publishThrottlingRateInMsg,
                    publishThrottlingRateInMsg, clockSource);
        } else {
            tokenBucketOnMessage = null;
        }
        if (publishThrottlingRateInByte > 0) {
            tokenBucketOnByte = new AsyncTokenBucket(BURST_FACTOR * publishThrottlingRateInByte,
                    publishThrottlingRateInByte, clockSource);
        } else {
            tokenBucketOnByte = null;
        }
    }
}
