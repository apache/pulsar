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
package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.qos.AsyncTokenBucketBuilder;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRateLimiterAsyncTokenBucketImpl extends DispatchRateLimiter {
    private volatile AsyncTokenBucket dispatchRateLimiterOnMessage;
    private volatile AsyncTokenBucket dispatchRateLimiterOnByte;

    public DispatchRateLimiterAsyncTokenBucketImpl(PersistentTopic topic, Type type) {
        this(topic, null, type);
    }

    public DispatchRateLimiterAsyncTokenBucketImpl(PersistentTopic topic, String subscriptionName, Type type) {
        super(topic, subscriptionName, type);
    }

    public DispatchRateLimiterAsyncTokenBucketImpl(BrokerService brokerService) {
        super(brokerService);
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    @Override
    public long getAvailableDispatchRateLimitOnMsg() {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage == null ? -1 :
                Math.max(localDispatchRateLimiterOnMessage.getTokens(), 0);
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    @Override
    public long getAvailableDispatchRateLimitOnByte() {
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte == null ? -1 : Math.max(localDispatchRateLimiterOnByte.getTokens(), 0);
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    @Override
    public void consumeDispatchQuota(long numberOfMessages, long byteSize) {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        if (numberOfMessages > 0 && localDispatchRateLimiterOnMessage != null) {
            localDispatchRateLimiterOnMessage.consumeTokens(numberOfMessages);
        }
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        if (byteSize > 0 && localDispatchRateLimiterOnByte != null) {
            localDispatchRateLimiterOnByte.consumeTokens(byteSize);
        }
    }

    /**
     * Checks if dispatch-rate limiting is enabled.
     *
     * @return
     */
    @Override
    public boolean isDispatchRateLimitingEnabled() {
        return dispatchRateLimiterOnMessage != null || dispatchRateLimiterOnByte != null;
    }

    /**
     * Update dispatch rate by updating msg and byte rate-limiter. If dispatch-rate is configured &lt; 0 then it closes
     * the rate-limiter and disables appropriate rate-limiter.
     *
     * @param dispatchRate
     */
    @Override
    public synchronized void updateDispatchRate(DispatchRate dispatchRate) {
        // synchronized to prevent race condition from concurrent zk-watch
        log.info("setting message-dispatch-rate {}", dispatchRate);

        long msgRate = dispatchRate.getDispatchThrottlingRateInMsg();
        long byteRate = dispatchRate.getDispatchThrottlingRateInByte();
        long ratePeriodNanos = TimeUnit.SECONDS.toNanos(Math.max(dispatchRate.getRatePeriodInSecond(), 1));

        // update msg-rateLimiter
        if (msgRate > 0) {
            if (dispatchRate.isRelativeToPublishRate()) {
                this.dispatchRateLimiterOnMessage =
                        configureAsyncTokenBucket(AsyncTokenBucket.builderForDynamicRate(), ratePeriodNanos)
                                .rateFunction(() -> getRelativeDispatchRateInMsg(dispatchRate))
                                .ratePeriodNanosFunction(() -> ratePeriodNanos)
                                .build();
            } else {
                this.dispatchRateLimiterOnMessage =
                        configureAsyncTokenBucket(AsyncTokenBucket.builder(), ratePeriodNanos)
                                .rate(msgRate).ratePeriodNanos(ratePeriodNanos)
                                .build();
            }
        } else {
            this.dispatchRateLimiterOnMessage = null;
        }

        // update byte-rateLimiter
        if (byteRate > 0) {
            if (dispatchRate.isRelativeToPublishRate()) {
                this.dispatchRateLimiterOnByte =
                        configureAsyncTokenBucket(AsyncTokenBucket.builderForDynamicRate(), ratePeriodNanos)
                                .rateFunction(() -> getRelativeDispatchRateInByte(dispatchRate))
                                .ratePeriodNanosFunction(() -> ratePeriodNanos)
                                .build();
            } else {
                this.dispatchRateLimiterOnByte =
                        configureAsyncTokenBucket(AsyncTokenBucket.builder(), ratePeriodNanos)
                                .rate(byteRate).ratePeriodNanos(ratePeriodNanos)
                                .build();
            }
        } else {
            this.dispatchRateLimiterOnByte = null;
        }
    }

    private <T extends AsyncTokenBucketBuilder<T>> T configureAsyncTokenBucket(T builder,
                                                                               long addTokensResolutionNanos) {
        builder.clock(brokerService.getPulsar().getMonotonicClock());
        // configures tokens to be added once in every addTokensResolutionNanos
        // this makes AsyncTokenBucket behave in the similar way as the "classic" dispatch rate limiter implementation
        // which uses a scheduled task to add tokens to the rate limiter once in every ratePeriod
        builder.addTokensResolutionNanos(addTokensResolutionNanos);
        return builder;
    }

    private long getRelativeDispatchRateInMsg(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInMsg() + dispatchRate.getDispatchThrottlingRateInMsg()
                : 0;
    }

    private long getRelativeDispatchRateInByte(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInByte() + dispatchRate.getDispatchThrottlingRateInByte()
                : 0;
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    @Override
    public long getDispatchRateOnMsg() {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage != null ? localDispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    @Override
    public long getDispatchRateOnByte() {
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte != null ? localDispatchRateLimiterOnByte.getRate() : -1;
    }


    @Override
    public void close() {
        // close rate-limiter
        if (dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DispatchRateLimiterAsyncTokenBucketImpl.class);
}
