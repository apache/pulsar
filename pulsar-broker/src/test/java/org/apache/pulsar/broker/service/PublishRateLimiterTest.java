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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    private DefaultThreadFactory threadFactory = new DefaultThreadFactory("pulsar-io");
    private EventLoop eventLoop = new DefaultEventLoop(threadFactory);

    @BeforeMethod
    public void setup() throws Exception {
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
    }

    @Test
    public void testPublishRateLimiterImplUpdate() {
        publishRateLimiter.handlePublishThrottling(producer, 11, 110);
        assertEquals(throttleTracker.throttledCount(), 1);

        // update
        throttleTracker = new ServerCnxThrottleTracker(serverCnx);
        publishRateLimiter.update(newPublishRate);
        publishRateLimiter.handlePublishThrottling(producer, 11, 110);
        assertEquals(throttleTracker.throttledCount(), 0);
    }
}
