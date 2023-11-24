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

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.RateLimiter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
public class PublishRateLimiterTest {
    private final String CLUSTER_NAME = "clusterName";
    private final Policies policies = new Policies();
    private final PublishRate publishRate = new PublishRate(10, 100);
    private final PublishRate newPublishRate = new PublishRate(20, 200);

    private PrecisePublishLimiter precisePublishLimiter;
    private PublishRateLimiterImpl publishRateLimiter;

    @BeforeMethod
    public void setup() throws Exception {
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put(CLUSTER_NAME, publishRate);

        precisePublishLimiter = new PrecisePublishLimiter(policies, CLUSTER_NAME, () -> System.out.print("Refresh permit"));
        publishRateLimiter = new PublishRateLimiterImpl(policies, CLUSTER_NAME);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        policies.publishMaxMessageRate.clear();
        policies.publishMaxMessageRate = null;
        precisePublishLimiter.close();
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

    @Test
    public void testPrecisePublishRateLimiterUpdate() {
        assertFalse(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(15, 150, null));

        //update
        precisePublishLimiter.update(newPublishRate);
        assertTrue(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(15, 150, null));
    }

    @Test
    public void testPrecisePublishRateLimiterAcquire() throws Exception {
        Class precisePublishLimiterClass = Class.forName("org.apache.pulsar.broker.service.PrecisePublishLimiter");
        Field topicPublishRateLimiterOnMessageField = precisePublishLimiterClass.getDeclaredField("topicPublishRateLimiterOnMessage");
        Field topicPublishRateLimiterOnByteField = precisePublishLimiterClass.getDeclaredField("topicPublishRateLimiterOnByte");
        topicPublishRateLimiterOnMessageField.setAccessible(true);
        topicPublishRateLimiterOnByteField.setAccessible(true);

        RateLimiter topicPublishRateLimiterOnMessage = (RateLimiter)topicPublishRateLimiterOnMessageField.get(
                precisePublishLimiter);
        RateLimiter topicPublishRateLimiterOnByte = (RateLimiter)topicPublishRateLimiterOnByteField.get(
                precisePublishLimiter);

        Method renewTopicPublishRateLimiterOnMessageMethod = topicPublishRateLimiterOnMessage.getClass().getDeclaredMethod("renew", null);
        Method renewTopicPublishRateLimiterOnByteMethod = topicPublishRateLimiterOnByte.getClass().getDeclaredMethod("renew", null);
        renewTopicPublishRateLimiterOnMessageMethod.setAccessible(true);
        renewTopicPublishRateLimiterOnByteMethod.setAccessible(true);

        // running tryAcquire in order to lazyInit the renewTask
        precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(1, 10, null);

        Field onMessageRenewTaskField = topicPublishRateLimiterOnMessage.getClass().getDeclaredField("renewTask");
        Field onByteRenewTaskField = topicPublishRateLimiterOnByte.getClass().getDeclaredField("renewTask");
        onMessageRenewTaskField.setAccessible(true);
        onByteRenewTaskField.setAccessible(true);
        ScheduledFuture<?> onMessageRenewTask = (ScheduledFuture<?>) onMessageRenewTaskField.get(topicPublishRateLimiterOnMessage);
        ScheduledFuture<?> onByteRenewTask = (ScheduledFuture<?>) onByteRenewTaskField.get(topicPublishRateLimiterOnByte);

        onMessageRenewTask.cancel(false);
        onByteRenewTask.cancel(false);

        // renewing the permits from previous tests
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire not exceeded
        assertTrue(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(1, 10, null));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire numOfMessages exceeded
        assertFalse(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(11, 100, null));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire msgSizeInBytes exceeded
        assertFalse(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(10, 101, null));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire exceeded exactly
        assertFalse(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(10, 100, null));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire not exceeded
        assertTrue(precisePublishLimiter.incrementPublishCountAndThrottleWhenNeeded(9, 99, null));
    }
}
