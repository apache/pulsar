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
package org.apache.pulsar.common.util;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is not a precise rate-limiter, but its performance is better.
 * We have defined 2 thread pools:
 * `checkRateExceededMonitor` is responsible for continuously detecting whether the traffic has exceeded the threshold.
 * `resetRateMonitor` is responsible for resetting the permit count per second.
 * We can reduce the value of `brokerDispatchThrottlingRateTimeMillis`
 * to increase the frequency of detection(at least 1ms),
 * thereby improving the accuracy of rate-limiter.
 * The consumption is lower than that of detecting every request.
 */
public abstract class AbstractScheduledRateLimiter implements AutoCloseable, ScheduledRateLimiter {
    protected ScheduledThreadPoolExecutor checkRateExceededMonitor;
    protected ScheduledThreadPoolExecutor resetRateMonitor;
    protected ScheduledFuture<?> checkRateScheduledFuture;
    protected ScheduledFuture<?> resetRateScheduledFuture;
    protected volatile boolean consumeRateExceeded = false;

    protected long maxMsgRate;
    protected long maxByteRate;
    protected long rateTime;

    public AbstractScheduledRateLimiter(long maxMsgRate, long maxByteRate, long rateTime) {
        this.maxMsgRate = maxMsgRate;
        this.maxByteRate = maxByteRate;
        this.rateTime = rateTime;
    }

    protected void updateRateExceededMonitor(long rateTime) {
        rateTime = Math.max(rateTime, 0);
        if (checkRateScheduledFuture != null) {
            checkRateScheduledFuture.cancel(false);
            checkRateScheduledFuture = null;
        }
        if (isDispatchRateLimitingEnabled() && rateTime > 0) {
            if (checkRateExceededMonitor == null) {
                checkRateExceededMonitor = new ScheduledThreadPoolExecutor(1,
                        new DefaultThreadFactory("pulsar-broker-consume-rate-monitor"));
                checkRateExceededMonitor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
                checkRateExceededMonitor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            }
            checkRateScheduledFuture = checkRateExceededMonitor.scheduleAtFixedRate(
                    this::checkConsumeRate, 0, rateTime, TimeUnit.MILLISECONDS);
        }
    }
    abstract public void checkConsumeRate();

    @Override
    public void updateDispatchRate(long maxMsgRate, long maxByteRate, long rateTime) {
        this.maxMsgRate = Math.max(maxMsgRate, 0);
        this.maxByteRate = Math.max(maxByteRate, 0);

        doUpdate();

        updateRateExceededMonitor(rateTime);
        updateRestRateMonitor();
    }

    abstract protected void doUpdate();

    protected void updateRestRateMonitor() {
        if (isDispatchRateLimitingEnabled()) {
            if (resetRateMonitor == null) {
                resetRateMonitor = new ScheduledThreadPoolExecutor(1,
                        new DefaultThreadFactory("pulsar-broker-consume-rate-reset-monitor"));
                resetRateMonitor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
                resetRateMonitor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            }
            if (resetRateScheduledFuture == null) {
                resetRateScheduledFuture = resetRateMonitor.scheduleAtFixedRate(
                        this::resetConsumeCount, 1, 1, TimeUnit.SECONDS);
            }
        } else {
            if (resetRateScheduledFuture != null) {
                resetRateScheduledFuture.cancel(false);
                resetRateScheduledFuture = null;
            }
        }
    }
    /**
     * Reset the permits of rate-limiter.
     */
    public abstract void resetConsumeCount();

    @Override
    public boolean isConsumeRateExceeded() {
        return consumeRateExceeded;
    }

    @Override
    public void close() {
        if (checkRateExceededMonitor != null) {
            checkRateExceededMonitor.shutdownNow();
        }
        if (resetRateMonitor != null) {
            resetRateMonitor.shutdownNow();
        }
    }

    public long getMaxMsgRate() {
        return maxMsgRate;
    }

    public long getMaxByteRate() {
        return maxByteRate;
    }
}
