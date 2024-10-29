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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.testng.annotations.Test;

public class ResourceGroupRateLimiterManagerTest {

    @Test
    public void testNewReplicationDispatchRateLimiterWithEmptyResourceGroup() {
        org.apache.pulsar.common.policies.data.ResourceGroup emptyResourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();

        @Cleanup(value = "shutdown")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        @Cleanup
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(emptyResourceGroup,executorService);
        assertFalse(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1L);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testReplicationDispatchRateLimiterOnMsgs() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs(10L);
        @Cleanup(value = "shutdown")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        @Cleanup
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup,executorService);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());


        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testReplicationDispatchRateLimiterOnBytes() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInBytes(20L);
        @Cleanup(value = "shutdown")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        @Cleanup
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup,executorService);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1L);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
    }

    @Test
    public void testUpdateReplicationDispatchRateLimiter() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs(10L);
        resourceGroup.setReplicationDispatchRateInBytes(100L);

        @Cleanup(value = "shutdown")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        @Cleanup
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup,executorService);

        BytesAndMessagesCount quota = new BytesAndMessagesCount();
        quota.messages = 20;
        quota.bytes = 200;
        ResourceGroupRateLimiterManager.updateReplicationDispatchRateLimiter(resourceGroupDispatchLimiter, quota);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), quota.bytes);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), quota.bytes);
        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), quota.messages);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), quota.messages);
    }
}
