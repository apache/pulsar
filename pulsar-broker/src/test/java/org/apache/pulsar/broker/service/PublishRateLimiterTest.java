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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
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

    private PublishRateLimiterImpl publishRateLimiter;

    @BeforeMethod
    public void setup() throws Exception {
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);

        publishRateLimiter = new PublishRateLimiterImpl(policies, CLUSTER_NAME);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        policies.publishMaxMessageRate.clear();
        policies.publishMaxMessageRate = null;
        publishRateLimiter.close();
    }

    @Test
    public void testPublishRateLimiterImplExceed() throws Exception {
        // increment not exceed
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(5, 50, null);
        publishRateLimiter.calculateThrottlingPauseNanos();
        assertFalse(publishRateLimiter.isPublishRateExceeded());
        publishRateLimiter.resetPublishCount();

        // numOfMessages increment exceeded
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(11, 100, null);
        publishRateLimiter.calculateThrottlingPauseNanos();
        assertTrue(publishRateLimiter.isPublishRateExceeded());
        publishRateLimiter.resetPublishCount();

        // msgSizeInBytes increment exceeded
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(9, 110, null);
        publishRateLimiter.calculateThrottlingPauseNanos();
        assertTrue(publishRateLimiter.isPublishRateExceeded());

    }

    @Test
    public void testPublishRateLimiterImplUpdate() {
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(11, 110, null);
        publishRateLimiter.calculateThrottlingPauseNanos();
        assertTrue(publishRateLimiter.isPublishRateExceeded());

        // update
        publishRateLimiter.update(newPublishRate);
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(11, 110, null);
        publishRateLimiter.calculateThrottlingPauseNanos();
        assertFalse(publishRateLimiter.isPublishRateExceeded());

    }
}
