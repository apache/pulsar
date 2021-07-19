/**
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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public class PrecisPublishLimiter implements PublishRateLimiter {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    // precise mode for publish rate limiter
    private volatile HashMap<String, RateLimiter> rateLimiters;
    private final RateLimitFunction rateLimitFunction;
    private static final String MESSAGE_RATE = "messageRate";
    private static final String BYTE_RATE = "byteRate";

    public PrecisPublishLimiter(Policies policies, String clusterName, RateLimitFunction rateLimitFunction) {
        this.rateLimitFunction = rateLimitFunction;
        this.rateLimiters = new HashMap<>();
        update(policies, clusterName);
    }

    public PrecisPublishLimiter(PublishRate publishRate, RateLimitFunction rateLimitFunction) {
        this.rateLimitFunction = rateLimitFunction;
        this.rateLimiters = new HashMap<>();
        update(publishRate);
    }

    @Override
    public void checkPublishRate() {
       // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
       // No-op
    }

    @Override
    public boolean resetPublishCount() {
        return true;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    private void releaseThrottle() {
        for (RateLimiter rateLimiter : rateLimiters.values()) {
            if (rateLimiter.getAvailablePermits() <= 0) {
                return;
            }
        }
        this.rateLimitFunction.apply();
    }

    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        this.update(maxPublishRate);
    }
    public void update(PublishRate maxPublishRate) {
        if (maxPublishRate != null
                && (maxPublishRate.publishThrottlingRateInMsg > 0 || maxPublishRate.publishThrottlingRateInByte > 0)) {
            this.publishThrottlingEnabled = true;
            this.publishMaxMessageRate = Math.max(maxPublishRate.publishThrottlingRateInMsg, 0);
            this.publishMaxByteRate = Math.max(maxPublishRate.publishThrottlingRateInByte, 0);
            if (this.publishMaxMessageRate > 0) {
                rateLimiters.put(MESSAGE_RATE,
                        new RateLimiter(publishMaxMessageRate
                                , 1, TimeUnit.SECONDS, this::releaseThrottle));
            }
            if (this.publishMaxByteRate > 0) {
                rateLimiters.put(BYTE_RATE,
                        new RateLimiter(publishMaxMessageRate
                                , 1, TimeUnit.SECONDS, this::releaseThrottle));
            }
        } else {
            this.publishMaxMessageRate = 0;
            this.publishMaxByteRate = 0;
            this.publishThrottlingEnabled = false;
            rateLimiters.put(MESSAGE_RATE, null);
            rateLimiters.put(BYTE_RATE, null);
        }
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        return (rateLimiters.get(MESSAGE_RATE) == null || rateLimiters.get(MESSAGE_RATE).tryAcquire(numbers))
                && (rateLimiters.get(BYTE_RATE) == null || rateLimiters.get(BYTE_RATE).tryAcquire(bytes));
    }
}
