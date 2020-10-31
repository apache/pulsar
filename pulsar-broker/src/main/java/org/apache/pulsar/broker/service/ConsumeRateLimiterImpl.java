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


import org.apache.pulsar.common.util.AbstractScheduledRateLimiter;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumeRateLimiterImpl extends AbstractScheduledRateLimiter {
    private LongAdder currentDispatchRateOnMessage = new LongAdder();
    private LongAdder currentDispatchRateOnByte = new LongAdder();
    private final Lock lock = new ReentrantLock();

    public ConsumeRateLimiterImpl(long maxMsgRate, long maxByteRate, long rateTime) {
        super(maxMsgRate, maxByteRate, rateTime);
        updateDispatchRate(maxMsgRate, maxByteRate, rateTime);
    }

    @Override
    public boolean incrementConsumeCount(long msgPermits, long bytePermits) {
        if (!isConsumeRateExceeded() && isDispatchRateLimitingEnabled()) {
            currentDispatchRateOnMessage.add(msgPermits);
            currentDispatchRateOnByte.add(bytePermits);
            return true;
        }
        return false;
    }

    @Override
    public boolean hasMessageDispatchPermit() {
        return isDispatchRateLimitingEnabled() && !isConsumeRateExceeded();
    }

    @Override
    public boolean isDispatchRateLimitingEnabled() {
        return currentDispatchRateOnMessage != null || currentDispatchRateOnByte != null;
    }

    @Override
    public void doUpdate() {
        if (maxMsgRate > 0 && currentDispatchRateOnMessage == null) {
            currentDispatchRateOnMessage = new LongAdder();
        } else if (maxMsgRate <= 0) {
            //disable dispatchRateLimiterOnMessage
            currentDispatchRateOnMessage = null;
        }

        if (maxByteRate > 0 && currentDispatchRateOnByte == null) {
            currentDispatchRateOnByte = new LongAdder();
        } else if (maxByteRate <= 0) {
            currentDispatchRateOnByte = null;
        }
        resetConsumeCount();
    }

    @Override
    public void resetConsumeCount() {
        try {
            lock.lock();
            if (currentDispatchRateOnMessage != null) {
                currentDispatchRateOnMessage.reset();
            }
            if (currentDispatchRateOnByte != null) {
                currentDispatchRateOnByte.reset();
            }
            consumeRateExceeded = false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void checkConsumeRate() {
        try {
            lock.lock();
            if (isDispatchRateLimitingEnabled() && !isConsumeRateExceeded()) {
                if (currentDispatchRateOnMessage.sum() >= maxMsgRate
                        || currentDispatchRateOnByte.sum() >= maxByteRate) {
                    consumeRateExceeded = true;
                }
            }
        } finally {
            lock.unlock();
        }
    }

}
