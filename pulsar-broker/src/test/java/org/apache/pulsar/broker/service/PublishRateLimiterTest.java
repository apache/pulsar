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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PublishRateLimiterTest {
    private final String CLUSTER_NAME = "clusterName";
    private final Policies policies = new Policies();
    private final PublishRate publishRate = new PublishRate(10, 100);
    private final PublishRate newPublishRate = new PublishRate(20, 200);
    private AtomicLong manualClockSource;

    private Producer producer;
    private PublishRateLimiterImpl publishRateLimiter;

    private AtomicInteger throttleCount = new AtomicInteger(0);

    @BeforeMethod
    public void setup() throws Exception {
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        publishRateLimiter = new PublishRateLimiterImpl(requestSnapshot -> manualClockSource.get());
        publishRateLimiter.update(policies, CLUSTER_NAME);
        producer = mock(Producer.class);
        throttleCount.set(0);
        doAnswer(a -> {
            throttleCount.incrementAndGet();
            return null;
        }).when(producer).incrementThrottleCount();
        doAnswer(a -> {
            throttleCount.decrementAndGet();
            return null;
        }).when(producer).decrementThrottleCount();
        TransportCnx transportCnx = mock(TransportCnx.class);
        when(producer.getCnx()).thenReturn(transportCnx);
        BrokerService brokerService = mock(BrokerService.class);
        when(transportCnx.getBrokerService()).thenReturn(brokerService);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(eventLoopGroup);
        doReturn(null).when(eventLoopGroup).schedule(any(Runnable.class), anyLong(), any());
        incrementSeconds(1);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        policies.publishMaxMessageRate.clear();
        policies.publishMaxMessageRate = null;
    }

    private void incrementSeconds(int seconds) {
        manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }

    @Test
    public void testPublishRateLimiterImplExceed() throws Exception {
        // increment not exceed
        publishRateLimiter.handlePublishThrottling(producer, 5, 50);
        assertEquals(throttleCount.get(), 0);

        incrementSeconds(1);

        // numOfMessages increment exceeded
        publishRateLimiter.handlePublishThrottling(producer, 11, 100);
        assertEquals(throttleCount.get(), 1);

        incrementSeconds(1);

        // msgSizeInBytes increment exceeded
        publishRateLimiter.handlePublishThrottling(producer, 9, 110);
        assertEquals(throttleCount.get(), 2);
    }

    @Test
    public void testPublishRateLimiterImplUpdate() {
        publishRateLimiter.handlePublishThrottling(producer, 11, 110);
        assertEquals(throttleCount.get(), 1);

        // update
        throttleCount.set(0);
        publishRateLimiter.update(newPublishRate);
        publishRateLimiter.handlePublishThrottling(producer, 11, 110);
        assertEquals(throttleCount.get(), 0);
    }
}
