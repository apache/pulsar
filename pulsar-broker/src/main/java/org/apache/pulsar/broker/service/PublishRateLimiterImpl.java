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

package org.apache.pulsar.broker.service;

import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.AsyncTokenBucket;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

public class PublishRateLimiterImpl implements PublishRateLimiter {
    private static final int DEFAULT_CONSISTENT_VIEW_INTERVAL = 10;
    private static int consistentViewOfTokensIntervalInUnthrottleLoop = DEFAULT_CONSISTENT_VIEW_INTERVAL;
    private volatile AsyncTokenBucket tokenBucketOnMessage;
    private volatile AsyncTokenBucket tokenBucketOnByte;
    private final LongSupplier clockSource;

    private final MessagePassingQueue<Producer> unthrottlingQueue = new MpscUnboundedArrayQueue<>(1024);

    private final AtomicBoolean unthrottlingScheduled = new AtomicBoolean(false);

    public static void switchToConsistentTokensView() {
        consistentViewOfTokensIntervalInUnthrottleLoop = 1;
    }

    public static void resetConsistentTokensViewToDefault() {
        consistentViewOfTokensIntervalInUnthrottleLoop = DEFAULT_CONSISTENT_VIEW_INTERVAL;
    }

    public PublishRateLimiterImpl(Policies policies, String clusterName) {
        this();
        update(policies, clusterName);
    }

    public PublishRateLimiterImpl(PublishRate maxPublishRate) {
        this();
        update(maxPublishRate);
    }

    public PublishRateLimiterImpl() {
        this(AsyncTokenBucket.DEFAULT_CLOCK_SOURCE);
    }

    public PublishRateLimiterImpl(LongSupplier clockSource) {
        this.clockSource = clockSource;
    }

    @Override
    public void handlePublishThrottling(Producer producer, int numOfMessages,
                                        long msgSizeInBytes) {
        boolean shouldThrottle = false;
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        if (currentTokenBucketOnMessage != null) {
            currentTokenBucketOnMessage.consumeTokens(numOfMessages);
            shouldThrottle = !currentTokenBucketOnMessage.containsTokens();
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            currentTokenBucketOnByte.consumeTokens(msgSizeInBytes);
            shouldThrottle = shouldThrottle || !currentTokenBucketOnByte.containsTokens();
        }
        if (shouldThrottle) {
            producer.incrementThrottleCount();
            scheduleDecrementThrottleCount(producer);
        }
    }

    private void scheduleDecrementThrottleCount(Producer producer) {
        unthrottlingQueue.offer(producer);
        if (unthrottlingScheduled.compareAndSet(false, true)) {
            EventLoopGroup executor = producer.getCnx().getBrokerService().executor();
            scheduleUnthrottling(executor);
        }
    }

    private void scheduleUnthrottling(ScheduledExecutorService executor) {
        long delay = calculatePause();
        executor.schedule(() -> this.unthrottleQueuedProducers(executor), delay, TimeUnit.NANOSECONDS);
    }

    private void unthrottleQueuedProducers(ScheduledExecutorService executor) {
        Producer producer;
        int handledProducersCount = 0;
        while (containsTokens(handledProducersCount++ % consistentViewOfTokensIntervalInUnthrottleLoop == 0)
                && (producer = unthrottlingQueue.poll()) != null) {
            producer.decrementThrottleCount();
        }
        if (!unthrottlingQueue.isEmpty()) {
            scheduleUnthrottling(executor);
        } else {
            unthrottlingScheduled.set(false);
        }
    }

    private long calculatePause() {
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        long pauseNanos = 0L;
        if (currentTokenBucketOnMessage != null) {
            pauseNanos = currentTokenBucketOnMessage.calculatePause(true);
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            pauseNanos = Math.max(pauseNanos,
                    currentTokenBucketOnByte.calculatePause(true));
        }
        return pauseNanos;
    }

    private boolean containsTokens(boolean forceUpdateTokens) {
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        if (currentTokenBucketOnMessage != null && !currentTokenBucketOnMessage.containsTokens(forceUpdateTokens)) {
            return false;
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null && !currentTokenBucketOnByte.containsTokens(forceUpdateTokens)) {
            return false;
        }
        return true;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        update(maxPublishRate);
    }

    public void update(PublishRate maxPublishRate) {
        if (maxPublishRate != null) {
            updateTokenBuckets(maxPublishRate.publishThrottlingRateInMsg, maxPublishRate.publishThrottlingRateInByte);
        } else {
            tokenBucketOnMessage = null;
            tokenBucketOnByte = null;
        }
    }

    protected void updateTokenBuckets(long publishThrottlingRateInMsg, long publishThrottlingRateInByte) {
        if (publishThrottlingRateInMsg > 0) {
            tokenBucketOnMessage =
                    AsyncTokenBucket.builder().rate(publishThrottlingRateInMsg).clockSource(clockSource).build();
        } else {
            tokenBucketOnMessage = null;
        }
        if (publishThrottlingRateInByte > 0) {
            tokenBucketOnByte =
                    AsyncTokenBucket.builder().rate(publishThrottlingRateInByte).clockSource(clockSource).build();
        } else {
            tokenBucketOnByte = null;
        }
    }
}
