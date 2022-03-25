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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.base.MoreObjects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Builder;

/**
 * A Rate Limiter that distributes permits at a configurable rate. Each {@link #acquire()} blocks if necessary until a
 * permit is available, and then takes it. Each {@link #tryAcquire()} tries to acquire permits from available permits,
 * it returns true if it succeed else returns false. Rate limiter release configured permits at every configured rate
 * time, so, on next ticket new fresh permits will be available.
 *
 * <p>For example: if RateLimiter is configured to release 10 permits at every 1 second then RateLimiter will allow to
 * acquire 10 permits at any time with in that 1 second.
 *
 * <p>Comparison with other RateLimiter such as {@link com.google.common.util.concurrent.RateLimiter}
 * <ul>
 * <li><b>Per second rate-limiting:</b> Per second rate-limiting not satisfied by Guava-RateLimiter</li>
 * <li><b>Guava RateLimiter:</b> For X permits: it releases X/1000 permits every msec. therefore,
 * for permits=2/sec =&gt; it release 1st permit on first 500msec and 2nd permit on next 500ms. therefore,
 * if 2 request comes with in 500msec duration then 2nd request fails to acquire permit
 * though we have configured 2 permits/second.</li>
 * <li><b>RateLimiter:</b> it releases X permits every second. so, in above usecase:
 * if 2 requests comes at the same time then both will acquire the permit.</li>
 * <li><b>Faster: </b>RateLimiter is light-weight and faster than Guava-RateLimiter</li>
 * </ul>
 */
public class RateLimiter implements AutoCloseable{
    private final ScheduledExecutorService executorService;
    private long rateTime;
    private TimeUnit timeUnit;
    private final boolean externalExecutor;
    private ScheduledFuture<?> renewTask;
    private volatile long permits;
    private volatile long acquiredPermits;
    private boolean isClosed;
    // permitUpdate helps to update permit-rate at runtime
    private Supplier<Long> permitUpdater;
    private RateLimitFunction rateLimitFunction;
    private boolean isDispatchOrPrecisePublishRateLimiter;

    @Builder
    RateLimiter(final ScheduledExecutorService scheduledExecutorService, final long permits, final long rateTime,
            final TimeUnit timeUnit, Supplier<Long> permitUpdater, boolean isDispatchOrPrecisePublishRateLimiter,
                       RateLimitFunction rateLimitFunction) {
        checkArgument(permits > 0, "rate must be > 0");
        checkArgument(rateTime > 0, "Renew permit time must be > 0");

        this.rateTime = rateTime;
        this.timeUnit = timeUnit;
        this.permits = permits;
        this.permitUpdater = permitUpdater;
        this.isDispatchOrPrecisePublishRateLimiter = isDispatchOrPrecisePublishRateLimiter;

        if (scheduledExecutorService != null) {
            this.executorService = scheduledExecutorService;
            this.externalExecutor = true;
        } else {
            final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.executorService = executor;
            this.externalExecutor = false;
        }

        this.rateLimitFunction = rateLimitFunction;

    }

    // default values for Lombok generated builder class
    public static class RateLimiterBuilder {
        private long rateTime = 1;
        private TimeUnit timeUnit = TimeUnit.SECONDS;
    }

    @Override
    public synchronized void close() {
        if (!isClosed) {
            if (!externalExecutor) {
                executorService.shutdownNow();
            }
            if (renewTask != null) {
                renewTask.cancel(false);
            }
            isClosed = true;
            // If there is a ratelimit function registered, invoke it to unblock.
            if (rateLimitFunction != null) {
                rateLimitFunction.apply();
            }
        }
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request be granted.
     *
     * <p>This method is equivalent to {@code acquire(1)}.
     */
    public synchronized void acquire() throws InterruptedException {
        acquire(1);
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request be granted.
     *
     * @param acquirePermit
     *            the number of permits to acquire
     */
    public synchronized void acquire(long acquirePermit) throws InterruptedException {
        checkArgument(!isClosed(), "Rate limiter is already shutdown");
        checkArgument(acquirePermit <= this.permits,
                "acquiring permits must be less or equal than initialized rate =" + this.permits);

        // lazy init and start task only once application start using it
        if (renewTask == null) {
            renewTask = createTask();
        }

        boolean canAcquire = false;
        do {
            canAcquire = acquirePermit < 0 || acquiredPermits < this.permits;
            if (!canAcquire) {
                wait();
            } else {
                acquiredPermits += acquirePermit;
            }
        } while (!canAcquire);
    }

    /**
     * Acquires permits from this {@link RateLimiter} if it can be acquired immediately without delay.
     *
     * <p>This method is equivalent to {@code tryAcquire(1)}.
     *
     * @return {@code true} if the permits were acquired, {@code false} otherwise
     */
    public synchronized boolean tryAcquire() {
        return tryAcquire(1);
    }

    /**
     * Acquires permits from this {@link RateLimiter} if it can be acquired immediately without delay.
     *
     * @param acquirePermit
     *            the number of permits to acquire
     * @return {@code true} if the permits were acquired, {@code false} otherwise
     */
    public synchronized boolean tryAcquire(long acquirePermit) {
        checkArgument(!isClosed(), "Rate limiter is already shutdown");
        // lazy init and start task only once application start using it
        if (renewTask == null) {
            renewTask = createTask();
        }

        boolean canAcquire = acquirePermit < 0 || acquiredPermits < this.permits;
        if (isDispatchOrPrecisePublishRateLimiter) {
            // for dispatch rate limiter just add acquirePermit
            acquiredPermits += acquirePermit;

            // we want to back-pressure from the current state of the rateLimiter therefore we should check if there
            // are any available premits again
            canAcquire = acquirePermit < 0 || acquiredPermits < this.permits;
        } else {
            // acquired-permits can't be larger than the rate
            if (acquirePermit > this.permits) {
                acquiredPermits = this.permits;
                return false;
            }

            if (canAcquire) {
                acquiredPermits += acquirePermit;
            }
        }

        return canAcquire;
    }

    /**
     * Return available permits for this {@link RateLimiter}.
     *
     * @return returns 0 if permits is not available
     */
    public long getAvailablePermits() {
        return Math.max(0, this.permits - this.acquiredPermits);
    }

    /**
     * Resets new rate by configuring new value for permits per configured rate-period.
     *
     * @param permits
     */
    public synchronized void setRate(long permits) {
        this.permits = permits;
    }

    /**
     * Resets new rate with new permits and rate-time.
     *
     * @param permits
     * @param rateTime
     * @param timeUnit
     * @param permitUpdaterByte
     */
    public synchronized void setRate(long permits, long rateTime, TimeUnit timeUnit, Supplier<Long> permitUpdaterByte) {
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        this.permits = permits;
        this.rateTime = rateTime;
        this.timeUnit = timeUnit;
        this.permitUpdater = permitUpdaterByte;
        this.renewTask = createTask();
    }

    /**
     * Returns configured permit rate per pre-configured rate-period.
     *
     * @return rate
     */
    public synchronized long getRate() {
        return this.permits;
    }

    public synchronized long getRateTime() {
        return this.rateTime;
    }

    public synchronized TimeUnit getRateTimeUnit() {
        return this.timeUnit;
    }

    protected ScheduledFuture<?> createTask() {
        return executorService.scheduleAtFixedRate(catchingAndLoggingThrowables(this::renew), this.rateTime,
                this.rateTime, this.timeUnit);
    }

    synchronized void renew() {
        acquiredPermits = isDispatchOrPrecisePublishRateLimiter ? Math.max(0, acquiredPermits - permits) : 0;
        if (permitUpdater != null) {
            long newPermitRate = permitUpdater.get();
            if (newPermitRate > 0) {
                setRate(newPermitRate);
            }
        }
        // release the back-pressure by applying the rateLimitFunction only when there are available permits
        if (rateLimitFunction != null && this.getAvailablePermits() > 0) {
            rateLimitFunction.apply();
        }
        notifyAll();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("rateTime", rateTime).add("permits", permits)
                .add("acquiredPermits", acquiredPermits).toString();
    }

}
