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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PublishRateLimiterTest {
    private static final String CLUSTER_NAME = "clusterName";
    private final Policies policies = new Policies();
    private final PublishRate publishRate = new PublishRate(10, 100);
    private final PublishRate newPublishRate = new PublishRate(20, 200);
    private AtomicLong manualClockSource;

    private Producer producer;
    private ServerCnx serverCnx;
    private PublishRateLimiterImpl publishRateLimiter;
    private ServerCnxThrottleTracker throttleTracker;
    private final DefaultThreadFactory threadFactory = new DefaultThreadFactory("pulsar-io");
    private EventLoop eventLoop;

    @BeforeMethod
    public void setup() throws Exception {
        eventLoop = new DefaultEventLoop(threadFactory);
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        publishRateLimiter = new PublishRateLimiterImpl(() -> manualClockSource.get(),
                producer -> {
                    producer.getCnx().getThrottleTracker().markThrottled(
                            ServerCnxThrottleTracker.ThrottleType.TopicPublishRate);
                }, producer -> {
            producer.getCnx().getThrottleTracker().unmarkThrottled(
                    ServerCnxThrottleTracker.ThrottleType.TopicPublishRate);
        });
        publishRateLimiter.update(policies, CLUSTER_NAME);
        producer = mock(Producer.class);
        serverCnx = mock(ServerCnx.class);
        ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
        doAnswer(a -> eventLoop).when(channelHandlerContext).executor();
        doAnswer(a -> channelHandlerContext).when(serverCnx).ctx();
        doAnswer(a -> this.serverCnx).when(producer).getCnx();
        throttleTracker = new ServerCnxThrottleTracker(this.serverCnx);
        doAnswer(a -> throttleTracker).when(this.serverCnx).getThrottleTracker();
        when(producer.getCnx()).thenReturn(serverCnx);
        BrokerService brokerService = mock(BrokerService.class);
        when(serverCnx.getBrokerService()).thenReturn(brokerService);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(eventLoopGroup);
        when(eventLoopGroup.next()).thenReturn(eventLoop);
        incrementSeconds(1);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        policies.publishMaxMessageRate.clear();
        policies.publishMaxMessageRate = null;
    }

    @AfterMethod
    public void tearDown() throws Exception {
        eventLoop.shutdownGracefully();
    }

    private void incrementSeconds(int seconds) {
        manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }

    @Test
    public void testPublishRateLimiterImplExceed() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                // increment not exceed
                publishRateLimiter.handlePublishThrottling(producer, 5, 50);
                assertEquals(throttleTracker.throttledCount(), 0);

                incrementSeconds(1);

                // numOfMessages increment exceeded
                publishRateLimiter.handlePublishThrottling(producer, 11, 100);
                assertEquals(throttleTracker.throttledCount(), 1);

                incrementSeconds(1);

                // msgSizeInBytes increment exceeded
                publishRateLimiter.handlePublishThrottling(producer, 9, 110);
                assertEquals(throttleTracker.throttledCount(), 2);

                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPublishRateLimiterImplUpdate() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                publishRateLimiter.handlePublishThrottling(producer, 11, 110);
                assertEquals(throttleTracker.throttledCount(), 1);

                // update
                throttleTracker = new ServerCnxThrottleTracker(serverCnx);
                publishRateLimiter.update(newPublishRate);
                publishRateLimiter.handlePublishThrottling(producer, 11, 110);
                assertEquals(throttleTracker.throttledCount(), 0);

                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    /**
     * When the token bucket is deeply depleted, the first scheduled unthrottle uses a long delay. Disabling limits
     * must schedule an immediate unthrottle (delay 0) so producers are not stuck until that delay elapses.
     */
    @Test
    public void shouldUnthrottleImmediatelyAfterDisablingLimitsDespiteLongPendingDelay() {
        AtomicLong manualClock = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        AtomicInteger unthrottleCalls = new AtomicInteger();

        PublishRateLimiterImpl limiter = new PublishRateLimiterImpl(
                manualClock::get,
                p -> { },
                p -> unthrottleCalls.incrementAndGet());

        EventLoop scheduler = mock(EventLoop.class);
        AtomicInteger longDelaySchedules = new AtomicInteger();
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            long delay = invocation.getArgument(1);
            TimeUnit unit = invocation.getArgument(2);
            long delayNanos = unit.toNanos(delay);
            if (delayNanos == 0L) {
                task.run();
            } else {
                longDelaySchedules.incrementAndGet();
            }
            @SuppressWarnings("unchecked")
            ScheduledFuture<?> scheduled = mock(ScheduledFuture.class);
            return scheduled;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any());

        Producer p = mock(Producer.class);
        ServerCnx cnx = mock(ServerCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        doAnswer(a -> ctx).when(cnx).ctx();
        doAnswer(a -> cnx).when(p).getCnx();
        when(p.getCnx()).thenReturn(cnx);
        doAnswer(a -> {
            ((Runnable) a.getArgument(0)).run();
            return null;
        }).when(cnx).execute(any(Runnable.class));

        BrokerService brokerService = mock(BrokerService.class);
        when(cnx.getBrokerService()).thenReturn(brokerService);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(eventLoopGroup);
        when(eventLoopGroup.next()).thenReturn(scheduler);

        limiter.update(new PublishRate(1, 0));
        manualClock.addAndGet(TimeUnit.SECONDS.toNanos(1));

        limiter.handlePublishThrottling(p, 100_000, 0L);
        assertEquals(unthrottleCalls.get(), 0);
        assertTrue(longDelaySchedules.get() >= 1,
                "Expected a long-delay unthrottle to be scheduled while the bucket is deeply depleted");

        limiter.update(new PublishRate(0, 0));
        assertEquals(unthrottleCalls.get(), 1);
    }

    /**
     * Relaxing only the byte limit still invalidates a previously scheduled long unthrottle delay; an immediate
     * unthrottle pass must run after buckets are rebuilt.
     */
    @Test
    public void shouldUnthrottleImmediatelyAfterRaisingByteLimitDespiteLongPendingDelay() {
        AtomicLong manualClock = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        AtomicInteger unthrottleCalls = new AtomicInteger();

        PublishRateLimiterImpl limiter = new PublishRateLimiterImpl(
                manualClock::get,
                p -> { },
                p -> unthrottleCalls.incrementAndGet());

        EventLoop scheduler = mock(EventLoop.class);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            long delay = invocation.getArgument(1);
            TimeUnit unit = invocation.getArgument(2);
            if (unit.toNanos(delay) == 0L) {
                task.run();
            }
            @SuppressWarnings("unchecked")
            ScheduledFuture<?> scheduled = mock(ScheduledFuture.class);
            return scheduled;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any());

        Producer p = mock(Producer.class);
        ServerCnx cnx = mock(ServerCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        doAnswer(a -> ctx).when(cnx).ctx();
        doAnswer(a -> cnx).when(p).getCnx();
        when(p.getCnx()).thenReturn(cnx);
        doAnswer(a -> {
            ((Runnable) a.getArgument(0)).run();
            return null;
        }).when(cnx).execute(any(Runnable.class));

        BrokerService brokerService = mock(BrokerService.class);
        when(cnx.getBrokerService()).thenReturn(brokerService);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(eventLoopGroup);
        when(eventLoopGroup.next()).thenReturn(scheduler);

        limiter.update(new PublishRate(0, 1));
        manualClock.addAndGet(TimeUnit.SECONDS.toNanos(1));

        limiter.handlePublishThrottling(p, 0, 100_000L);
        assertEquals(unthrottleCalls.get(), 0);

        limiter.update(new PublishRate(0, 1_000_000));
        assertEquals(unthrottleCalls.get(), 1);

        AsyncTokenBucket byteBucket = limiter.getTokenBucketOnByte();
        assertNotNull(byteBucket);
    }
}
