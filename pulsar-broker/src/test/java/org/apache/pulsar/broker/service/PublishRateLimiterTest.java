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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.PublishRateLimiter.ThrottleInstruction;
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

    private PublishSource publishSource = new PublishSource(mock(TransportCnx.class), mock(Topic.class));
    private PublishRateLimiterImpl publishRateLimiter;

    @BeforeMethod
    public void setup() throws Exception {
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        publishRateLimiter = new PublishRateLimiterImpl(manualClockSource::get);
        publishRateLimiter.update(policies, CLUSTER_NAME);
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
        ThrottleInstruction throttleInstruction = publishRateLimiter.consumePublishQuota(publishSource, 5, 50);
        assertFalse(throttleInstruction.shouldThrottle());

        incrementSeconds(1);

        // numOfMessages increment exceeded
        throttleInstruction = publishRateLimiter.consumePublishQuota(publishSource, 11, 100);
        assertTrue(throttleInstruction.shouldThrottle());

        incrementSeconds(1);

        // msgSizeInBytes increment exceeded
        throttleInstruction = publishRateLimiter.consumePublishQuota(publishSource, 9, 110);
        assertTrue(throttleInstruction.shouldThrottle());
    }

    @Test
    public void testPublishRateLimiterImplUpdate() {
        ThrottleInstruction throttleInstruction = publishRateLimiter.consumePublishQuota(publishSource, 11, 110);
        assertTrue(throttleInstruction.shouldThrottle());

        // update
        publishRateLimiter.update(newPublishRate);
        throttleInstruction = publishRateLimiter.consumePublishQuota(publishSource, 11, 110);
        assertFalse(throttleInstruction.shouldThrottle());
    }
}
