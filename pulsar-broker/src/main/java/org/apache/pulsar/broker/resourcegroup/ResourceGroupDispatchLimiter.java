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
package org.apache.pulsar.broker.resourcegroup;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.RateLimiter;

public class ResourceGroupDispatchLimiter implements AutoCloseable {

    private final ScheduledExecutorService executorService;
    private volatile RateLimiter dispatchRateLimiterOnMessage;
    private volatile RateLimiter dispatchRateLimiterOnByte;

    public ResourceGroupDispatchLimiter(ScheduledExecutorService executorService,
                                        long dispatchRateInMsgs, long dispatchRateInBytes) {
        this.executorService = executorService;
        update(dispatchRateInMsgs, dispatchRateInBytes);
    }

    public void update(long dispatchRateInMsgs, long dispatchRateInBytes) {
        if (dispatchRateInMsgs > 0) {
            if (dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.setRate(dispatchRateInMsgs);
            } else {
                this.dispatchRateLimiterOnMessage =
                        RateLimiter.builder()
                                .scheduledExecutorService(executorService)
                                .permits(dispatchRateInMsgs)
                                .rateTime(1)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(null)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            }
        } else {
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        if (dispatchRateInBytes > 0) {
            if (dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.setRate(dispatchRateInBytes);
            } else {
                this.dispatchRateLimiterOnByte =
                        RateLimiter.builder()
                                .scheduledExecutorService(executorService)
                                .permits(dispatchRateInBytes)
                                .rateTime(1)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(null)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            }
        } else {
            if (this.dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.close();
                this.dispatchRateLimiterOnByte = null;
            }
        }
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        return dispatchRateLimiterOnMessage == null ? -1 : dispatchRateLimiterOnMessage.getAvailablePermits();
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnByte() {
        return dispatchRateLimiterOnByte == null ? -1 : dispatchRateLimiterOnByte.getAvailablePermits();
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    public void consumeDispatchQuota(long numberOfMessages, long byteSize) {
        if (numberOfMessages > 0 && dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage.tryAcquire(numberOfMessages);
        }
        if (byteSize > 0 && dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte.tryAcquire(byteSize);
        }
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    public boolean tryAcquire(long numberOfMessages, long byteSize) {
        if (numberOfMessages > 0 && dispatchRateLimiterOnMessage != null) {
            if (!dispatchRateLimiterOnMessage.tryAcquire(numberOfMessages)) {
                return false;
            }
        }
        if (byteSize > 0 && dispatchRateLimiterOnByte != null) {
            if (!dispatchRateLimiterOnByte.tryAcquire(byteSize)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if dispatch-rate limiting is enabled.
     *
     * @return
     */
    public boolean isDispatchRateLimitingEnabled() {
        return dispatchRateLimiterOnMessage != null || dispatchRateLimiterOnByte != null;
    }

    public void close() {
        if (dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte = null;
        }
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnMsg() {
        return dispatchRateLimiterOnMessage != null ? dispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnByte() {
        return dispatchRateLimiterOnByte != null ? dispatchRateLimiterOnByte.getRate() : -1;
    }


}
