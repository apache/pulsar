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


import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class ConsumeRateLimiterTest {

    @BeforeMethod
    public void setup() {
    }

    @Test
    public void testRateUpdate() {
        ConsumeRateLimiterImpl consumeRateLimiter = new ConsumeRateLimiterImpl(1024, 100, 10000);
        Assert.assertEquals(consumeRateLimiter.getMaxMsgRate(), 1024);
        Assert.assertEquals(consumeRateLimiter.getMaxByteRate(), 100);

        consumeRateLimiter.updateDispatchRate(2048, 120, 10000);
        Assert.assertEquals(consumeRateLimiter.getMaxMsgRate(), 2048);
        Assert.assertEquals(consumeRateLimiter.getMaxByteRate(), 120);

        Assert.assertTrue(consumeRateLimiter.incrementConsumeCount(200, 2048));
        consumeRateLimiter.checkConsumeRate();
        Assert.assertFalse(consumeRateLimiter.incrementConsumeCount(200, 2048));

        consumeRateLimiter.close();
    }

    @Test
    public void testRateEnable() {
        ConsumeRateLimiterImpl consumeRateLimiter = new ConsumeRateLimiterImpl(0, 0, 10000);
        Assert.assertFalse(consumeRateLimiter.isDispatchRateLimitingEnabled());

        consumeRateLimiter.updateDispatchRate(1024, 1, 10000);
        Assert.assertTrue(consumeRateLimiter.isDispatchRateLimitingEnabled());

        consumeRateLimiter.close();
    }

    @Test
    public void testThrottling() throws Exception {
        ConsumeRateLimiterImpl consumeRateLimiter = new ConsumeRateLimiterImpl(1024, 100, 10000);
        Assert.assertTrue(consumeRateLimiter.incrementConsumeCount(200, 2048));
        consumeRateLimiter.checkConsumeRate();
        Assert.assertFalse(consumeRateLimiter.incrementConsumeCount(200, 2048));

        //wait for reset
        Thread.sleep(1100);
        Assert.assertTrue(consumeRateLimiter.incrementConsumeCount(200, 2048));
        Assert.assertTrue(consumeRateLimiter.incrementConsumeCount(200, 2048));
        Assert.assertTrue(consumeRateLimiter.incrementConsumeCount(200, 2048));
        Assert.assertTrue(consumeRateLimiter.hasMessageDispatchPermit());
        Assert.assertFalse(consumeRateLimiter.isConsumeRateExceeded());

        consumeRateLimiter.checkConsumeRate();
        Assert.assertFalse(consumeRateLimiter.hasMessageDispatchPermit());

        consumeRateLimiter.close();
    }

    @Test
    public void testUpdateMonitor() throws Exception {
        ConsumeRateLimiterImpl consumeRateLimiter = new ConsumeRateLimiterImpl(0, 0, 10000);
        Assert.assertNull(getMonitor(consumeRateLimiter, "resetRateMonitor"));
        Assert.assertNull(getMonitor(consumeRateLimiter, "checkRateExceededMonitor"));
        Assert.assertNull(getScheduledFuture(consumeRateLimiter, "resetRateScheduledFuture"));
        Assert.assertNull(getScheduledFuture(consumeRateLimiter, "checkRateScheduledFuture"));
        consumeRateLimiter.close();

        consumeRateLimiter = new ConsumeRateLimiterImpl(10, 1024, 10000);
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "resetRateMonitor"));
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "checkRateExceededMonitor"));
        Assert.assertNotNull(getScheduledFuture(consumeRateLimiter, "resetRateScheduledFuture"));
        Assert.assertNotNull(getScheduledFuture(consumeRateLimiter, "checkRateScheduledFuture"));

        consumeRateLimiter.updateDispatchRate(0, 0, 1000);
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "resetRateMonitor"));
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "checkRateExceededMonitor"));
        Assert.assertNull(getScheduledFuture(consumeRateLimiter, "resetRateScheduledFuture"));
        Assert.assertNull(getScheduledFuture(consumeRateLimiter, "checkRateScheduledFuture"));

        consumeRateLimiter.updateDispatchRate(10, 1024, 1000);
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "resetRateMonitor"));
        Assert.assertNotNull(getMonitor(consumeRateLimiter, "checkRateExceededMonitor"));
        Assert.assertNotNull(getScheduledFuture(consumeRateLimiter, "resetRateScheduledFuture"));
        Assert.assertNotNull(getScheduledFuture(consumeRateLimiter, "checkRateScheduledFuture"));

        Thread.sleep(1100);
        Assert.assertTrue(getMonitor(consumeRateLimiter, "resetRateMonitor")
                .getCompletedTaskCount() >= 1);
        Assert.assertTrue(getMonitor(consumeRateLimiter, "checkRateExceededMonitor")
                .getCompletedTaskCount() >= 1);

        consumeRateLimiter.close();
    }

    @Test
    public void testMonitor() throws Exception {
        ConsumeRateLimiterImpl consumeRateLimiter = new ConsumeRateLimiterImpl(1024, 100, 10000);
        Assert.assertFalse(consumeRateLimiter.isConsumeRateExceeded());
        // 1) stop the monitor
        consumeRateLimiter.updateDispatchRate(0, 0, 1);
        consumeRateLimiter.incrementConsumeCount(1000, 2000);
        Thread.sleep(1200);
        Assert.assertFalse(consumeRateLimiter.isConsumeRateExceeded());

        // 2) start the monitor
        consumeRateLimiter.updateDispatchRate(100, 100, 1);
        consumeRateLimiter.incrementConsumeCount(1000, 2000);
        Thread.sleep(100);
        Assert.assertTrue(consumeRateLimiter.isConsumeRateExceeded());
        //wait for reset
        Thread.sleep(1000);
        Assert.assertFalse(consumeRateLimiter.isConsumeRateExceeded());

        consumeRateLimiter.close();
    }

    private ScheduledThreadPoolExecutor getMonitor(ConsumeRateLimiterImpl consumeRateLimiter, String monitorName) throws Exception {
        Field monitorField = ConsumeRateLimiterImpl.class.getSuperclass().getDeclaredField(monitorName);
        monitorField.setAccessible(true);
        return (ScheduledThreadPoolExecutor) monitorField.get(consumeRateLimiter);
    }

    private ScheduledFuture<?> getScheduledFuture(ConsumeRateLimiterImpl consumeRateLimiter, String futureName) throws Exception {
        Field monitorField = ConsumeRateLimiterImpl.class.getSuperclass().getDeclaredField(futureName);
        monitorField.setAccessible(true);
        return (ScheduledFuture<?>) monitorField.get(consumeRateLimiter);
    }
}
