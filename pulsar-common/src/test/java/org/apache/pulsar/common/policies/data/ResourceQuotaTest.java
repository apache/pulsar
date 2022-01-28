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
package org.apache.pulsar.common.policies.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ResourceQuotaTest {
    @Test
    public void testResourceQuotaDefault() {
        ResourceQuota quota = new ResourceQuota();
        Assert.assertEquals(quota.getMsgRateIn(), 0.0);
        Assert.assertEquals(quota.getMsgRateOut(), 0.0);
        Assert.assertEquals(quota.getBandwidthIn(), 0.0);
        Assert.assertEquals(quota.getBandwidthOut(), 0.0);
        Assert.assertEquals(quota.getMemory(), 0.0);
        Assert.assertTrue(quota.getDynamic());

        quota.setMsgRateIn(10);
        quota.setMsgRateOut(20);
        quota.setBandwidthIn(10000);
        quota.setBandwidthOut(20000);
        quota.setMemory(100);
        quota.setDynamic(false);
        Assert.assertEquals(quota.getMsgRateIn(), 10.0);
        Assert.assertEquals(quota.getMsgRateOut(), 20.0);
        Assert.assertEquals(quota.getBandwidthIn(), 10000.0);
        Assert.assertEquals(quota.getBandwidthOut(), 20000.0);
        Assert.assertEquals(quota.getMemory(), 100.0);
        Assert.assertFalse(quota.getDynamic());
    }

    @Test
    public void testResourceQuotaEqual() {
        ResourceQuota quota1 = new ResourceQuota();
        quota1.setMsgRateIn(10);
        quota1.setMsgRateOut(20);
        quota1.setBandwidthIn(10000);
        quota1.setBandwidthOut(20000);
        quota1.setMemory(100);
        quota1.setDynamic(false);

        ResourceQuota quota2 = new ResourceQuota();
        Assert.assertNotEquals(quota1, quota2);

        quota2.setMsgRateIn(10);
        Assert.assertNotEquals(quota1, quota2);

        quota2.setMsgRateOut(20);
        Assert.assertNotEquals(quota1, quota2);

        quota2.setBandwidthIn(10000);
        Assert.assertNotEquals(quota1, quota2);

        quota2.setBandwidthOut(20000);
        Assert.assertNotEquals(quota1, quota2);

        quota2.setMemory(100);
        Assert.assertNotEquals(quota1, quota2);

        quota2.setDynamic(false);
        Assert.assertEquals(quota1, quota2);
    }
}
