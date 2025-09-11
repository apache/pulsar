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
package org.apache.pulsar.common.util;

import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimedSingleThreadRateLimiter {

    @Getter
    private final int rate;
    @Getter
    private final long periodAtMs;
    private long lastTimeReset;
    @Getter
    private int remaining;
    private long closeAfterAtMs;

    public TimedSingleThreadRateLimiter(final int rate, final long period, final TimeUnit unit) {
        this.rate = rate;
        this.periodAtMs = unit.toMillis(period);
        this.lastTimeReset = System.currentTimeMillis();
        this.remaining = rate;
    }

    public int acquire(int permits) {
        final long now = System.currentTimeMillis();
        if (now > closeAfterAtMs) {
            return permits;
        }
        mayRenew(now);
        if (remaining > permits) {
            remaining -= permits;
            log.info("acquired acquired: {}, remaining:{}", permits, remaining);
            return permits;
        } else {
            int acquired = remaining;
            remaining = 0;
            log.info("acquired acquired: {}, remaining:{}", acquired, remaining);
            return acquired;
        }
    }

    public void timingOpen(long closeAfter, final TimeUnit unit) {
        this.closeAfterAtMs = System.currentTimeMillis() + unit.toMillis(closeAfter);
    }

    private void mayRenew(long now) {
        if (now > lastTimeReset + periodAtMs) {
            remaining = rate;
            lastTimeReset = now;
        }
    }
}
