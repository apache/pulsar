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
package org.apache.pulsar.broker.resourcegroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public class ResourceGroupPublishLimiter implements PublishRateLimiter, RateLimitFunction, AutoCloseable  {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    private volatile RateLimiter publishRateLimiterOnMessage;
    private volatile RateLimiter publishRateLimiterOnByte;
    private final ScheduledExecutorService scheduledExecutorService;

    ConcurrentHashMap<String, RateLimitFunction> rateLimitFunctionMap = new ConcurrentHashMap<>();

    public ResourceGroupPublishLimiter(ResourceGroup resourceGroup, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        update(resourceGroup);
    }

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
        return true;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public void update(Policies policies, String clusterName) {
      // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
      // No-op
    }

    public void update(ResourceGroup resourceGroup) {
        replaceLimiters(() -> {
            if (resourceGroup != null
                && (resourceGroup.getPublishRateInMsgs() > 0 || resourceGroup.getPublishRateInBytes() > 0)) {
                this.publishThrottlingEnabled = true;
                this.publishMaxMessageRate = Math.max(resourceGroup.getPublishRateInMsgs(), 0);
                this.publishMaxByteRate = Math.max(resourceGroup.getPublishRateInBytes(), 0);
                if (this.publishMaxMessageRate > 0) {
                    // TODO: pass the executor
                    publishRateLimiterOnMessage =
                        new RateLimiter(publishMaxMessageRate, 1, TimeUnit.SECONDS, this::apply);
                }
                if (this.publishMaxByteRate > 0) {
                    // TODO: pass the executor
                    publishRateLimiterOnByte =
                        new RateLimiter(publishMaxByteRate, 1, TimeUnit.SECONDS, this::apply);
                }
            } else {
                this.publishMaxMessageRate = 0;
                this.publishMaxByteRate = 0;
                this.publishThrottlingEnabled = false;
                publishRateLimiterOnMessage = null;
                publishRateLimiterOnByte = null;
            }
        });
    }

    public boolean tryAcquire(int numbers, long bytes) {
        return (publishRateLimiterOnMessage == null || publishRateLimiterOnMessage.tryAcquire(numbers))
            && (publishRateLimiterOnByte == null || publishRateLimiterOnByte.tryAcquire(bytes));
    }

    public void registerRateLimitFunction(String name, RateLimitFunction func) {
        rateLimitFunctionMap.put(name, func);
    }

    public void unregisterRateLimitFunction(String name) {
        rateLimitFunctionMap.remove(name);
    }

    private void replaceLimiters(Runnable updater) {
        RateLimiter previousPublishRateLimiterOnMessage = publishRateLimiterOnMessage;
        publishRateLimiterOnMessage = null;
        RateLimiter previousPublishRateLimiterOnByte = publishRateLimiterOnByte;
        publishRateLimiterOnByte = null;
        try {
            if (updater != null) {
                updater.run();
            }
        } finally {
            // Close previous limiters to prevent resource leakages.
            // Delay closing of previous limiters after new ones are in place so that updating the limiter
            // doesn't cause unavailability.
            if (previousPublishRateLimiterOnMessage != null) {
                previousPublishRateLimiterOnMessage.close();
            }
            if (previousPublishRateLimiterOnByte != null) {
                previousPublishRateLimiterOnByte.close();
            }
        }
    }

    @Override
    public void close() {
      this.apply();
      replaceLimiters(null);
    }

    @Override
    public void apply() {
        for (Map.Entry<String, RateLimitFunction> entry: rateLimitFunctionMap.entrySet()) {
            entry.getValue().apply();
        }
    }
}