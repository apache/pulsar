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

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PublishRateLimiterTest {
    private final String CLUSTER_NAME = "clusterName";
    private final Policies policies = new Policies();
    private final PublishRate publishRate = new PublishRate(10, 100);
    private final PublishRate newPublishRate = new PublishRate(20, 200);

    private PrecisPublishLimiter precisPublishLimiter;
    private PublishRateLimiterImpl publishRateLimiter;


    @BeforeMethod
    public void setup() throws Exception {
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);
        precisPublishLimiter = new PrecisPublishLimiter(policies, CLUSTER_NAME,
                () -> System.out.print("Refresh permit"));
        publishRateLimiter = new PublishRateLimiterImpl(policies, CLUSTER_NAME);
    }

    @Test
    public void testPublishRateLimiterImplExceed() throws Exception {
        // increment not exceed
        publishRateLimiter.incrementPublishCount(5, 50);
        publishRateLimiter.checkPublishRate();
        assertFalse(publishRateLimiter.isPublishRateExceeded());
        publishRateLimiter.resetPublishCount();

        // numOfMessages increment exceeded
        publishRateLimiter.incrementPublishCount(11, 100);
        publishRateLimiter.checkPublishRate();
        assertTrue(publishRateLimiter.isPublishRateExceeded());
        publishRateLimiter.resetPublishCount();

        // msgSizeInBytes increment exceeded
        publishRateLimiter.incrementPublishCount(9, 110);
        publishRateLimiter.checkPublishRate();
        assertTrue(publishRateLimiter.isPublishRateExceeded());

    }

    @Test
    public void testPublishRateLimiterImplUpdate() throws Exception {
        publishRateLimiter.incrementPublishCount(11, 110);
        publishRateLimiter.checkPublishRate();
        assertTrue(publishRateLimiter.isPublishRateExceeded());

        // update
        publishRateLimiter.update(newPublishRate);
        publishRateLimiter.incrementPublishCount(11, 110);
        publishRateLimiter.checkPublishRate();
        assertFalse(publishRateLimiter.isPublishRateExceeded());

    }

    @Test
    public void testPrecisePublishRateLimiterUpdate() throws Exception {
        assertFalse(precisPublishLimiter.tryAcquire(15, 150));

        //update
        precisPublishLimiter.update(newPublishRate);
        assertTrue(precisPublishLimiter.tryAcquire(15, 150));
    }

    @Test
    public void testPrecisePublishRateLimiterAcquire() throws Exception {
        // tryAcquire not exceeded
        assertTrue(precisPublishLimiter.tryAcquire(1, 10));
        Thread.sleep(1100);

        // tryAcquire numOfMessages exceeded
        assertFalse(precisPublishLimiter.tryAcquire(11, 100));
        Thread.sleep(1100);

        // tryAcquire msgSizeInBytes exceeded
        assertFalse(precisPublishLimiter.tryAcquire(10, 101));
        Thread.sleep(1100);

        // tryAcquire not exceeded
        assertTrue(precisPublishLimiter.tryAcquire(10, 100));
    }
}
