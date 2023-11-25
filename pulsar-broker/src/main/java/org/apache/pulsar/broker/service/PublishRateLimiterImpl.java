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
    public static final int BURST_FACTOR = 2;
    public static final LongSupplier DEFAULT_CLOCK_SOURCE = System::nanoTime;
    private volatile AsyncTokenBucket tokenBucketOnMessage;
    private volatile AsyncTokenBucket tokenBucketOnByte;

    public PublishRateLimiterImpl(Policies policies, String clusterName) {
        update(policies, clusterName);
    }

    public PublishRateLimiterImpl(PublishRate maxPublishRate) {
        update(maxPublishRate);
    }

    @Override
    public void incrementPublishCountAndThrottleWhenNeeded(int numOfMessages, long msgSizeInBytes,
                                                           ThrottleHandler throttleHandler) {
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        long pauseNanos = 0L;
        if (currentTokenBucketOnMessage != null) {
            pauseNanos = currentTokenBucketOnMessage.updateAndConsumeTokensAndCalculatePause(numOfMessages);
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            pauseNanos = Math.max(pauseNanos,
                    currentTokenBucketOnByte.updateAndConsumeTokensAndCalculatePause(msgSizeInBytes));
        }
        if (pauseNanos > 0) {
            throttleHandler.accept(pauseNanos);
        }
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
            if (maxPublishRate.publishThrottlingRateInMsg > 0) {
                tokenBucketOnMessage = new AsyncTokenBucket(BURST_FACTOR * maxPublishRate.publishThrottlingRateInMsg,
                        maxPublishRate.publishThrottlingRateInMsg, DEFAULT_CLOCK_SOURCE);
            } else {
                tokenBucketOnMessage = null;
            }
            if (maxPublishRate.publishThrottlingRateInByte > 0) {
                tokenBucketOnByte = new AsyncTokenBucket(BURST_FACTOR * maxPublishRate.publishThrottlingRateInByte,
                        maxPublishRate.publishThrottlingRateInByte, DEFAULT_CLOCK_SOURCE);
            } else {
                tokenBucketOnByte = null;
            }
        } else {
            tokenBucketOnMessage = null;
            tokenBucketOnByte = null;
        }
    }
}
