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

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.qos.MonotonicSnapshotClock;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

public class PublishRateLimiterImpl implements PublishRateLimiter {
    private volatile AsyncTokenBucket tokenBucketOnMessage;
    private volatile AsyncTokenBucket tokenBucketOnByte;
    private final MonotonicSnapshotClock monotonicSnapshotClock;

    private final MessagePassingQueue<Producer> unthrottlingQueue = new MpscUnboundedArrayQueue<>(1024);

    private final AtomicInteger throttledProducersCount = new AtomicInteger(0);
    private final AtomicBoolean processingQueuedProducers = new AtomicBoolean(false);

    public PublishRateLimiterImpl(MonotonicSnapshotClock monotonicSnapshotClock) {
        this.monotonicSnapshotClock = monotonicSnapshotClock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handlePublishThrottling(Producer producer, int numOfMessages,
                                        long msgSizeInBytes) {
        boolean shouldThrottle = false;
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        if (currentTokenBucketOnMessage != null) {
            // consume tokens from the token bucket for messages
            // we should throttle if it returns false since the token bucket is empty in that case
            shouldThrottle = !currentTokenBucketOnMessage.consumeTokensAndCheckIfContainsTokens(numOfMessages);
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            // consume tokens from the token bucket for bytes
            // we should throttle if it returns false since the token bucket is empty in that case
            shouldThrottle |= !currentTokenBucketOnByte.consumeTokensAndCheckIfContainsTokens(msgSizeInBytes);
        }
        if (shouldThrottle) {
            // throttle the producer by incrementing the throttle count
            producer.incrementThrottleCount();
            // schedule decrementing the throttle count to possibly unthrottle the producer after the
            // throttling period
            scheduleDecrementThrottleCount(producer);
        }
    }

    private void scheduleDecrementThrottleCount(Producer producer) {
        // add the producer to the queue of producers to be unthrottled
        unthrottlingQueue.offer(producer);
        // schedule unthrottling when the throttling count is incremented to 1
        // this is to avoid scheduling unthrottling multiple times for concurrent producers
        if (throttledProducersCount.incrementAndGet() == 1) {
            EventLoopGroup executor = producer.getCnx().getBrokerService().executor();
            scheduleUnthrottling(executor, calculateThrottlingDurationNanos());
        }
    }

    /**
     * Schedules the unthrottling operation after a throttling period.
     *
     * This method will usually be called only once at a time. However, in a multi-threaded environment,
     * it's possible for concurrent threads to call this method simultaneously. This is acceptable and does not
     * disrupt the functionality, as the method is designed to handle such scenarios gracefully.
     *
     * The solution avoids using locks and this nonblocking approach requires allowing concurrent calls to this method.
     * The implementation intends to prevent skipping of scheduling as a result of a race condition, which could
     * result in a producer never being unthrottled.
     *
     * The solution for skipping of scheduling is to allow 2 threads to schedule unthrottling when the throttling
     * count is exactly 1 when unthrottleQueuedProducers checks whether there's a need to reschedule. There might
     * be another thread that added it and also scheduled unthrottling. This is acceptable and intended for resolving
     * the race condition.
     *
     * @param executor The executor service used to schedule the unthrottling operation.
     * @param delayNanos
     */
    private void scheduleUnthrottling(ScheduledExecutorService executor, long delayNanos) {
        executor.schedule(() -> this.unthrottleQueuedProducers(executor), delayNanos,
                TimeUnit.NANOSECONDS);
    }

    private long calculateThrottlingDurationNanos() {
        AsyncTokenBucket currentTokenBucketOnMessage = tokenBucketOnMessage;
        long throttlingDurationNanos = 0L;
        if (currentTokenBucketOnMessage != null) {
            throttlingDurationNanos = currentTokenBucketOnMessage.calculateThrottlingDuration();
        }
        AsyncTokenBucket currentTokenBucketOnByte = tokenBucketOnByte;
        if (currentTokenBucketOnByte != null) {
            throttlingDurationNanos = Math.max(throttlingDurationNanos,
                    currentTokenBucketOnByte.calculateThrottlingDuration());
        }
        return throttlingDurationNanos;
    }

    private void unthrottleQueuedProducers(ScheduledExecutorService executor) {
        if (!processingQueuedProducers.compareAndSet(false, true)) {
            // another thread is already processing unthrottling
            return;
        }
        try {
            Producer producer;
            long throttlingDuration = 0L;
            // unthrottle as many producers as possible while there are token available
            while ((throttlingDuration = calculateThrottlingDurationNanos()) == 0L
                    && (producer = unthrottlingQueue.poll()) != null) {
                producer.decrementThrottleCount();
                throttledProducersCount.decrementAndGet();
            }
            // if there are still producers to be unthrottled, schedule unthrottling again
            // after another throttling period
            if (throttledProducersCount.get() > 0) {
                scheduleUnthrottling(executor, throttlingDuration);
            }
        } finally {
            processingQueuedProducers.set(false);
        }
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
                    AsyncTokenBucket.builder().rate(publishThrottlingRateInMsg).clock(monotonicSnapshotClock).build();
        } else {
            tokenBucketOnMessage = null;
        }
        if (publishThrottlingRateInByte > 0) {
            tokenBucketOnByte =
                    AsyncTokenBucket.builder().rate(publishThrottlingRateInByte).clock(monotonicSnapshotClock).build();
        } else {
            tokenBucketOnByte = null;
        }
    }

    @VisibleForTesting
    public AsyncTokenBucket getTokenBucketOnMessage() {
        return tokenBucketOnMessage;
    }

    @VisibleForTesting
    public AsyncTokenBucket getTokenBucketOnByte() {
        return tokenBucketOnByte;
    }
}
