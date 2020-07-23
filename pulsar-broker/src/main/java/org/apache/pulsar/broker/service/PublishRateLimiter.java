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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public interface PublishRateLimiter {

    static PublishRateLimiter DISABLED_RATE_LIMITER = PublishRateLimiterDisable.DISABLED_RATE_LIMITER;

    /**
     * checks and update state of current publish and marks if it has exceeded the rate-limiting threshold.
     */
    void checkPublishRate();

    /**
     * increments current publish count.
     *
     * @param numOfMessages
     * @param msgSizeInBytes
     */
    void incrementPublishCount(int numOfMessages, long msgSizeInBytes);

    /**
     * reset current publish count.
     *
     * @return
     */
    boolean resetPublishCount();

    /**
     * returns true if current publish has reached the rate-limiting threshold.
     * @return
     */
    boolean isPublishRateExceeded();

    /**
     * updates rate-limiting threshold based on policies.
     * @param policies
     * @param clusterName
     */
    void update(Policies policies, String clusterName);

    /**
     * updates rate-limiting threshold based on passed in rate limiter.
     * @param maxPublishRate
     */
    void update(PublishRate maxPublishRate);

    /**
     * try to acquire permit
     * @param numbers
     * @param bytes
     * */
    boolean tryAcquire(int numbers, long bytes);
}

class PrecisPublishLimiter implements PublishRateLimiter {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    // precise mode for publish rate limiter
    private RateLimiter topicPublishRateLimiterOnMessage;
    private RateLimiter topicPublishRateLimiterOnByte;
    private final RateLimitFunction rateLimitFunction;

    public PrecisPublishLimiter(Policies policies, String clusterName, RateLimitFunction rateLimitFunction) {
        this.rateLimitFunction = rateLimitFunction;
        update(policies, clusterName);
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
                topicPublishRateLimiterOnMessage = new RateLimiter(publishMaxMessageRate, 1, TimeUnit.SECONDS, rateLimitFunction);
            }
            if (this.publishMaxByteRate > 0) {
                topicPublishRateLimiterOnByte = new RateLimiter(publishMaxByteRate, 1, TimeUnit.SECONDS);
            }
        } else {
            this.publishMaxMessageRate = 0;
            this.publishMaxByteRate = 0;
            this.publishThrottlingEnabled = false;
            topicPublishRateLimiterOnMessage = null;
            topicPublishRateLimiterOnByte = null;
        }
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        return (topicPublishRateLimiterOnMessage == null || topicPublishRateLimiterOnMessage.tryAcquire(numbers)) &&
                (topicPublishRateLimiterOnByte == null || topicPublishRateLimiterOnByte.tryAcquire(bytes));
    }
}

class PublishRateLimiterImpl implements PublishRateLimiter {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    protected volatile boolean publishRateExceeded = false;
    protected volatile LongAdder currentPublishMsgCount = new LongAdder();
    protected volatile LongAdder currentPublishByteCount = new LongAdder();

    public PublishRateLimiterImpl(Policies policies, String clusterName) {
        update(policies, clusterName);
    }

    public PublishRateLimiterImpl(PublishRate maxPublishRate) {
        update(maxPublishRate);
    }

    @Override
    public void checkPublishRate() {
        if (this.publishThrottlingEnabled && !publishRateExceeded) {
            long currentPublishMsgRate = this.currentPublishMsgCount.sum();
            long currentPublishByteRate = this.currentPublishByteCount.sum();
            if ((this.publishMaxMessageRate > 0 && currentPublishMsgRate > this.publishMaxMessageRate)
                    || (this.publishMaxByteRate > 0 && currentPublishByteRate > this.publishMaxByteRate)) {
                publishRateExceeded = true;
            }
        }
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        if (this.publishThrottlingEnabled) {
            this.currentPublishMsgCount.add(numOfMessages);
            this.currentPublishByteCount.add(msgSizeInBytes);
        }
    }

    @Override
    public boolean resetPublishCount() {
        if (this.publishThrottlingEnabled) {
            this.currentPublishMsgCount.reset();
            this.currentPublishByteCount.reset();
            this.publishRateExceeded = false;
            return true;
        }
        return false;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return publishRateExceeded;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        update(maxPublishRate);
    }

    public void update(PublishRate maxPublishRate) {
        if (maxPublishRate != null
            && (maxPublishRate.publishThrottlingRateInMsg > 0 || maxPublishRate.publishThrottlingRateInByte > 0)) {
            this.publishThrottlingEnabled = true;
            this.publishMaxMessageRate = Math.max(maxPublishRate.publishThrottlingRateInMsg, 0);
            this.publishMaxByteRate = Math.max(maxPublishRate.publishThrottlingRateInByte, 0);
        } else {
            this.publishMaxMessageRate = 0;
            this.publishMaxByteRate = 0;
            this.publishThrottlingEnabled = false;
        }
        resetPublishCount();
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        return false;
    }
}

class PublishRateLimiterDisable implements PublishRateLimiter {

    public static final PublishRateLimiterDisable DISABLED_RATE_LIMITER = new PublishRateLimiterDisable();

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
        // No-op
        return false;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
        // No-op
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        // No-op
        return false;
    }

}
