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
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;
import org.apache.pulsar.utils.StatsOutputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
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

        precisPublishLimiter = new PrecisPublishLimiter(policies, CLUSTER_NAME, () -> System.out.print("Refresh permit"));
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
    public void testPublishRateLimiterImplUpdate() {
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
    public void testPrecisePublishRateLimiterUpdate() {
        assertFalse(precisPublishLimiter.tryAcquire(15, 150));

        //update
        precisPublishLimiter.update(newPublishRate);
        assertTrue(precisPublishLimiter.tryAcquire(15, 150));
    }

    @Test
    public void testPrecisePublishRateLimiterAcquire() throws Exception {
        Class precisPublishLimiterClass = Class.forName("org.apache.pulsar.broker.service.PrecisPublishLimiter");
        Field topicPublishRateLimiterOnMessageField = precisPublishLimiterClass.getDeclaredField("topicPublishRateLimiterOnMessage");
        Field topicPublishRateLimiterOnByteField = precisPublishLimiterClass.getDeclaredField("topicPublishRateLimiterOnByte");
        topicPublishRateLimiterOnMessageField.setAccessible(true);
        topicPublishRateLimiterOnByteField.setAccessible(true);

        RateLimiter topicPublishRateLimiterOnMessage = (RateLimiter)topicPublishRateLimiterOnMessageField.get(precisPublishLimiter);
        RateLimiter topicPublishRateLimiterOnByte = (RateLimiter)topicPublishRateLimiterOnByteField.get(precisPublishLimiter);

        Method renewTopicPublishRateLimiterOnMessageMethod = topicPublishRateLimiterOnMessage.getClass().getDeclaredMethod("renew", null);
        Method renewTopicPublishRateLimiterOnByteMethod = topicPublishRateLimiterOnByte.getClass().getDeclaredMethod("renew", null);
        renewTopicPublishRateLimiterOnMessageMethod.setAccessible(true);
        renewTopicPublishRateLimiterOnByteMethod.setAccessible(true);

        // running tryAcquire in order to lazyInit the renewTask
        precisPublishLimiter.tryAcquire(1, 10);

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
        assertTrue(precisPublishLimiter.tryAcquire(1, 10));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire numOfMessages exceeded
        assertFalse(precisPublishLimiter.tryAcquire(11, 100));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire msgSizeInBytes exceeded
        assertFalse(precisPublishLimiter.tryAcquire(10, 101));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire exceeded exactly
        assertFalse(precisPublishLimiter.tryAcquire(10, 100));
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);
        renewTopicPublishRateLimiterOnMessageMethod.invoke(topicPublishRateLimiterOnMessage);
        renewTopicPublishRateLimiterOnByteMethod.invoke(topicPublishRateLimiterOnByte);

        // tryAcquire not exceeded
        assertTrue(precisPublishLimiter.tryAcquire(9, 99));
    }
}
