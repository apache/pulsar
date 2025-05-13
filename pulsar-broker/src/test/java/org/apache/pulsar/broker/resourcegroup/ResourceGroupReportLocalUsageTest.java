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

        BytesAndMessagesCount dispatchBM = new BytesAndMessagesCount();
        dispatchBM.bytes = 20;
        dispatchBM.messages = 10;
        BytesAndMessagesCount publishBM = new BytesAndMessagesCount();
        publishBM.bytes = 30;
        publishBM.messages = 20;
        String replicator1RemoteCluster = "r1";
        BytesAndMessagesCount replicator1BM = new BytesAndMessagesCount();
        replicator1BM.bytes = 40;
        replicator1BM.messages = 30;
        String replicator2RemoteCluster = "r2";
        BytesAndMessagesCount replicator2BM = new BytesAndMessagesCount();
        replicator2BM.bytes = 50;
        replicator2BM.messages = 40;

        org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = service.resourceGroupGet(rgName);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.Dispatch, dispatchBM, null);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.Publish, publishBM, null);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.ReplicationDispatch, replicator1BM,
                replicator1RemoteCluster);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.ReplicationDispatch, replicator2BM,
                replicator2RemoteCluster);

        // Case1: Suppress report ResourceUsage.
        needReport.set(false);
        ResourceUsage resourceUsage = new ResourceUsage();
        resourceGroup.rgFillResourceUsage(resourceUsage);
        assertFalse(resourceUsage.hasDispatch());
        assertFalse(resourceUsage.hasPublish());
        assertEquals(resourceUsage.getReplicatorsCount(), 0);

        PerMonitoringClassFields dispatchMonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, null);
        assertEquals(dispatchMonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(dispatchMonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(dispatchMonitoredEntity.lastReportedValues.messages, 0);
        assertEquals(dispatchMonitoredEntity.lastReportedValues.bytes, 0);
        PerMonitoringClassFields publishMonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.Publish, null);
        assertEquals(publishMonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(publishMonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(publishMonitoredEntity.lastReportedValues.messages, 0);
        assertEquals(publishMonitoredEntity.lastReportedValues.bytes, 0);
        PerMonitoringClassFields r1MonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch,
                        replicator1RemoteCluster);
        assertEquals(r1MonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(r1MonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(r1MonitoredEntity.lastReportedValues.messages, 0);
        assertEquals(r1MonitoredEntity.lastReportedValues.bytes, 0);
        PerMonitoringClassFields r2MonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch,
                        replicator2RemoteCluster);
        assertEquals(r2MonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(r2MonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(r2MonitoredEntity.lastReportedValues.messages, 0);
        assertEquals(r2MonitoredEntity.lastReportedValues.bytes, 0);

        // Case2: Report ResourceUsage.
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.Dispatch, dispatchBM, null);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.Publish, publishBM, null);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.ReplicationDispatch, replicator1BM,
                replicator1RemoteCluster);
        resourceGroup.incrementLocalUsageStats(ResourceGroupMonitoringClass.ReplicationDispatch, replicator2BM,
                replicator2RemoteCluster);
        needReport.set(true);

        resourceUsage = new ResourceUsage();
        resourceGroup.rgFillResourceUsage(resourceUsage);
        assertTrue(resourceUsage.hasDispatch());
        assertTrue(resourceUsage.hasPublish());
        assertEquals(resourceUsage.getReplicatorsCount(), 2);

        dispatchMonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, null);
        assertEquals(dispatchMonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(dispatchMonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(dispatchMonitoredEntity.lastReportedValues.messages, dispatchBM.messages);
        assertEquals(dispatchMonitoredEntity.lastReportedValues.bytes, dispatchBM.bytes);
        publishMonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.Publish, null);
        assertEquals(publishMonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(publishMonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(publishMonitoredEntity.lastReportedValues.messages, publishBM.messages);
        assertEquals(publishMonitoredEntity.lastReportedValues.bytes, publishBM.bytes);
        r1MonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch,
                        replicator1RemoteCluster);
        assertEquals(r1MonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(r1MonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(r1MonitoredEntity.lastReportedValues.messages, replicator1BM.messages);
        assertEquals(r1MonitoredEntity.lastReportedValues.bytes, replicator1BM.bytes);
        r2MonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch,
                        replicator2RemoteCluster);
        assertEquals(r2MonitoredEntity.usedLocallySinceLastReport.messages, 0);
        assertEquals(r2MonitoredEntity.usedLocallySinceLastReport.bytes, 0);
        assertEquals(r2MonitoredEntity.lastReportedValues.messages, replicator2BM.messages);
        assertEquals(r2MonitoredEntity.lastReportedValues.bytes, replicator2BM.bytes);
    }
}