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
import java.util.function.Supplier;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRateLimiterClassicImpl extends DispatchRateLimiter {
    private volatile RateLimiter dispatchRateLimiterOnMessage;
    private volatile RateLimiter dispatchRateLimiterOnByte;

    public DispatchRateLimiterClassicImpl(PersistentTopic topic, Type type) {
        this(topic, null, type);
    }

    public DispatchRateLimiterClassicImpl(PersistentTopic topic, String subscriptionName, Type type) {
        super(topic, subscriptionName, type);
    }

    public DispatchRateLimiterClassicImpl(BrokerService brokerService) {
        super(brokerService);
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    @Override
    public long getAvailableDispatchRateLimitOnMsg() {
        RateLimiter localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage == null ? -1 :
                Math.max(localDispatchRateLimiterOnMessage.getAvailablePermits(), 0);
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    @Override
    public long getAvailableDispatchRateLimitOnByte() {
        RateLimiter localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte == null ? -1 :
                Math.max(localDispatchRateLimiterOnByte.getAvailablePermits(), 0);
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    @Override
    public void consumeDispatchQuota(long numberOfMessages, long byteSize) {
        RateLimiter localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        if (numberOfMessages > 0 && localDispatchRateLimiterOnMessage != null) {
            localDispatchRateLimiterOnMessage.tryAcquire(numberOfMessages);
        }
        RateLimiter localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        if (byteSize > 0 && localDispatchRateLimiterOnByte != null) {
            localDispatchRateLimiterOnByte.tryAcquire(byteSize);
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
        long ratePeriod = dispatchRate.getRatePeriodInSecond();

        Supplier<Long> permitUpdaterMsg = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInMsg(dispatchRate)
                : null;
        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.dispatchRateLimiterOnMessage == null) {
                this.dispatchRateLimiterOnMessage =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(msgRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterMsg)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            } else {
                this.dispatchRateLimiterOnMessage.setRate(msgRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterMsg);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        Supplier<Long> permitUpdaterByte = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInByte(dispatchRate)
                : null;
        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.dispatchRateLimiterOnByte == null) {
                this.dispatchRateLimiterOnByte =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(byteRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterByte)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            } else {
                this.dispatchRateLimiterOnByte.setRate(byteRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterByte);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.close();
                this.dispatchRateLimiterOnByte = null;
            }
        }
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
        RateLimiter localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage != null ? localDispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    @Override
    public long getDispatchRateOnByte() {
        RateLimiter localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte != null ? localDispatchRateLimiterOnByte.getRate() : -1;
    }

    @Override
    public void close() {
        // close rate-limiter
        if (dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage.close();
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte.close();
            dispatchRateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DispatchRateLimiterClassicImpl.class);
}
