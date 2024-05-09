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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.PerMonitoringClassFields;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ResourceGroupReportLocalUsageTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testRgFillResourceUsage() throws Exception {
        pulsar.getResourceGroupServiceManager().close();
        AtomicBoolean needReport = new AtomicBoolean(false);
        ResourceGroupService service = new ResourceGroupService(pulsar, TimeUnit.HOURS, null,
                new ResourceQuotaCalculator() {
                    @Override
                    public boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                                          long currentMessagesUsed, long lastReportedMessages,
                                                          long lastReportTimeMSecsSinceEpoch) {
                        return needReport.get();
                    }

                    @Override
                    public long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) {
                        return 0;
                    }
                });
        String rgName = "rg-1";
        ResourceGroup rgConfig = new ResourceGroup();
        rgConfig.setPublishRateInBytes(1000L);
        rgConfig.setPublishRateInMsgs(2000);
        service.resourceGroupCreate(rgName, rgConfig);

        BytesAndMessagesCount bytesAndMessagesCount = new BytesAndMessagesCount();
        bytesAndMessagesCount.bytes = 20;
        bytesAndMessagesCount.messages = 10;

        org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = service.resourceGroupGet(rgName);
        for (ResourceGroupMonitoringClass value : ResourceGroupMonitoringClass.values()) {
            resourceGroup.incrementLocalUsageStats(value, bytesAndMessagesCount);
        }

        // Case1: Suppress report ResourceUsage.
        needReport.set(false);
        ResourceUsage resourceUsage = new ResourceUsage();
        resourceGroup.rgFillResourceUsage(resourceUsage);
        assertFalse(resourceUsage.hasDispatch());
        assertFalse(resourceUsage.hasPublish());
        for (ResourceGroupMonitoringClass value : ResourceGroupMonitoringClass.values()) {
            PerMonitoringClassFields monitoredEntity =
                    resourceGroup.getMonitoredEntity(value);
            assertEquals(monitoredEntity.usedLocallySinceLastReport.messages, 0);
            assertEquals(monitoredEntity.usedLocallySinceLastReport.bytes, 0);
            assertEquals(monitoredEntity.totalUsedLocally.messages, 0);
            assertEquals(monitoredEntity.totalUsedLocally.bytes, 0);
            assertEquals(monitoredEntity.lastReportedValues.messages, 0);
            assertEquals(monitoredEntity.lastReportedValues.bytes, 0);
        }

        // Case2: Report ResourceUsage.
        for (ResourceGroupMonitoringClass value : ResourceGroupMonitoringClass.values()) {
            resourceGroup.incrementLocalUsageStats(value, bytesAndMessagesCount);
        }
        needReport.set(true);
        resourceUsage = new ResourceUsage();
        resourceGroup.rgFillResourceUsage(resourceUsage);
        assertTrue(resourceUsage.hasDispatch());
        assertTrue(resourceUsage.hasPublish());
        for (ResourceGroupMonitoringClass value : ResourceGroupMonitoringClass.values()) {
            PerMonitoringClassFields monitoredEntity =
                    resourceGroup.getMonitoredEntity(value);
            assertEquals(monitoredEntity.usedLocallySinceLastReport.messages, 0);
            assertEquals(monitoredEntity.usedLocallySinceLastReport.bytes, 0);
            assertEquals(monitoredEntity.totalUsedLocally.messages, bytesAndMessagesCount.messages);
            assertEquals(monitoredEntity.totalUsedLocally.bytes, bytesAndMessagesCount.bytes);
            assertEquals(monitoredEntity.lastReportedValues.messages, bytesAndMessagesCount.messages);
            assertEquals(monitoredEntity.lastReportedValues.bytes, bytesAndMessagesCount.bytes);
        }
    }
}