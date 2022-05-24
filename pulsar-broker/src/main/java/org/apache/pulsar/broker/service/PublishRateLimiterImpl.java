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

import java.util.concurrent.atomic.LongAdder;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;

public class PublishRateLimiterImpl implements PublishRateLimiter {
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
            if (this.publishMaxByteRate > 0) {
                long currentPublishByteRate = this.currentPublishByteCount.sum();
                if (currentPublishByteRate > this.publishMaxByteRate) {
                    publishRateExceeded = true;
                    return;
                }
            }

            if (this.publishMaxMessageRate > 0) {
                long currentPublishMsgRate = this.currentPublishMsgCount.sum();
                if (currentPublishMsgRate > this.publishMaxMessageRate) {
                    publishRateExceeded = true;
                }
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

    @Override
    public void close() {
        // no-op
    }
}
