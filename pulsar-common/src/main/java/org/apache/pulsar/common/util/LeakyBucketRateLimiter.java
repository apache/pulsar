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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
public class LeakyBucketRateLimiter extends AbstractRateLimiter{
    public LeakyBucketRateLimiter(ScheduledExecutorService service, long permits, long rateTime,
                                  TimeUnit timeUnit, Supplier<Long> permitUpdater) {
        super(service, permits, rateTime, timeUnit, permitUpdater);
    }

    public LeakyBucketRateLimiter(ScheduledExecutorService service, long permits, long rateTime,
                                  TimeUnit timeUnit) {
        this(service, permits, rateTime, timeUnit, null);
    }

    public LeakyBucketRateLimiter(long permits, long rateTime,
                                  TimeUnit timeUnit) {
        super(permits, rateTime, timeUnit);
    }

    public LeakyBucketRateLimiter(long permits, long rateTime,
                                  TimeUnit timeUnit, RateLimitFunction rateLimitFunction) {
        super(permits, rateTime, timeUnit, rateLimitFunction);
    }

    private synchronized boolean isUnfilled() {
        return acquiredPermits < this.permits;
    }

    @Override
    public synchronized boolean tryAcquire(long acquirePermit) {
        checkArgument(!isClosed(), "Rate limiter is already shutdown");
        // lazy init and start task only once application start using it
        if (renewTask == null) {
            renewTask = createTask();
        }

        acquiredPermits += acquirePermit;

        return acquirePermit < 0 || isUnfilled();
    }

    @Override
    synchronized void renew() {
        //Leaks every rate period with by the allowed permits size
        acquiredPermits = Math.max(0, acquiredPermits - permits);

        if (permitUpdater != null) {
            long newPermitRate = permitUpdater.get();
            if (newPermitRate > 0) {
                setRate(newPermitRate);
            }
        }

        if (rateLimitFunction != null && isUnfilled()) {
            rateLimitFunction.apply();
        }

        notifyAll();
    }
}
