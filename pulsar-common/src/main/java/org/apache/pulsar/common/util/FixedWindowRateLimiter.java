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

public class FixedWindowRateLimiter extends AbstractRateLimiter{

    public FixedWindowRateLimiter(ScheduledExecutorService service, long permits, long rateTime,
                                  TimeUnit timeUnit, Supplier<Long> permitUpdater) {
        super(service, permits, rateTime, timeUnit, permitUpdater);
    }

    public synchronized boolean tryAcquire(long acquirePermit) {
        checkArgument(!isClosed(), "Rate limiter is already shutdown");
        // lazy init and start task only once application start using it
        if (renewTask == null) {
            renewTask = createTask();
        }

        boolean canAcquire = acquirePermit < 0 || acquiredPermits < this.permits;
        // acquired-permits can't be larger than the rate
        if (acquirePermit > this.permits) {
            acquiredPermits = this.permits;
            return false;
        }

        if (canAcquire) {
            acquiredPermits += acquirePermit;
        }

        return canAcquire;
    }


    synchronized void renew() {
        acquiredPermits = 0;
        if (permitUpdater != null) {
            long newPermitRate = permitUpdater.get();
            if (newPermitRate > 0) {
                setRate(newPermitRate);
            }
        }
        if (rateLimitFunction != null) {
            rateLimitFunction.apply();
        }
        notifyAll();
    }
}
