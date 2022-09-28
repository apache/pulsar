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
package org.apache.pulsar.broker.resourcegroup;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.PerMonitoringClassFields;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ResourceGroupServiceTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.prepareData();

        ResourceQuotaCalculator dummyQuotaCalc = new ResourceQuotaCalculator() {
            @Override
            public boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                                  long currentMessagesUsed, long lastReportedMessages,
                                                  long lastReportTimeMSecsSinceEpoch)
            {
                final int maxSuppressRounds = ResourceGroupService.MaxUsageReportSuppressRounds;
                if (++numLocalReportsEvaluated % maxSuppressRounds == (maxSuppressRounds - 1)) {
                    return true;
                }

                return false;
            }

            @Override
            public long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) {
                // Note: as a side effect of simply incrementing/returning numAnonymousQuotaCalculations, we will also
                // set up the quota of bytes/messages accordingly in various rounds of calling into dummyQuotaCalc();
                // so, successive rounds will get (for quota <bytes, messages>): <1, 2>, <3, 4>, ...
                return ++numAnonymousQuotaCalculations;
            }

            private long numLocalReportsEvaluated;
        };

        // Somehow, pulsar.resourceUsageTransportManager is null here; work around for now.
        ResourceUsageTopicTransportManager transportMgr = new ResourceUsageTopicTransportManager(pulsar);
        this.rgs = new ResourceGroupService(pulsar, TimeUnit.MILLISECONDS, transportMgr, dummyQuotaCalc);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        log.info("numAnonymousQuotaCalculations={}", this.numAnonymousQuotaCalculations);

        super.internalCleanup();
    }

    // Measure the overhead for a lookup in the codepath, going through ResourceGroupService.
    @Test
    public void measureOpsTime() throws PulsarAdminException {
        long mSecsStart, mSecsEnd, diffMsecs;
        final int numPerfTestIterations = 10_000_000;
        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
          new org.apache.pulsar.common.policies.data.ResourceGroup();
        BytesAndMessagesCount stats = new BytesAndMessagesCount();
        ResourceGroupMonitoringClass monClass;
        final String rgName = "measureRGIncStatTime";

        rgs.resourceGroupCreate(rgName, rgConfig);
        ResourceGroup rg = rgs.resourceGroupGet("measureRGIncStatTime");

        // Direct op on the resource group
        mSecsStart = System.currentTimeMillis();
        for (int ix = 0; ix < numPerfTestIterations; ix++) {
            for (int monClassIdx = 0; monClassIdx < ResourceGroupMonitoringClass.values().length; monClassIdx++) {
                monClass = ResourceGroupMonitoringClass.values()[monClassIdx];
                rg.incrementLocalUsageStats(monClass, stats);
            }
        }
        mSecsEnd = System.currentTimeMillis();
        diffMsecs = mSecsEnd - mSecsStart;
        log.info("{} iterations of incrementLocalUsageStats on retRG in {} msecs ({} usecs for each)",
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs)/numPerfTestIterations);

        // Going through the resource-group service
        final String tenantName = "SomeTenant";
        final String namespaceName = "SomeNameSpace";
        rgs.registerTenant(rgName, tenantName);
        final NamespaceName tenantAndNamespaceName = NamespaceName.get(tenantName, namespaceName);
        rgs.registerNameSpace(rgName, tenantAndNamespaceName);
        mSecsStart = System.currentTimeMillis();
        for (int ix = 0; ix < numPerfTestIterations; ix++) {
            for (int monClassIdx = 0; monClassIdx < ResourceGroupMonitoringClass.values().length; monClassIdx++) {
                monClass = ResourceGroupMonitoringClass.values()[monClassIdx];
                rgs.incrementUsage(tenantName, namespaceName, monClass, stats);
            }
        }
        mSecsEnd = System.currentTimeMillis();
        diffMsecs = mSecsEnd - mSecsStart;
        log.info("{} iterations of incrementUsage on RGS in {} msecs ({} usecs for each)",
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs)/numPerfTestIterations);
        rgs.unRegisterTenant(rgName, tenantName);
        rgs.unRegisterNameSpace(rgName, tenantAndNamespaceName);

        // The overhead of a RG lookup
        mSecsStart = System.currentTimeMillis();
        for (int ix = 0; ix < numPerfTestIterations; ix++) {
            rgs.resourceGroupGet(rg.resourceGroupName);
        }
        mSecsEnd = System.currentTimeMillis();
        diffMsecs = mSecsEnd - mSecsStart;
        log.info("{} iterations of GET on RGS in {} msecs ({} usecs for each)",
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs)/numPerfTestIterations);

        rgs.resourceGroupDelete(rgName);
    }

    @Test
    public void testResourceGroupOps() throws PulsarAdminException, InterruptedException {
        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
          new org.apache.pulsar.common.policies.data.ResourceGroup();
        final String rgName = "testRG";
        final String randomRgName = "Something";
        rgConfig.setPublishRateInBytes(15000L);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(40000L);
        rgConfig.setDispatchRateInMsgs(500);

        int initialNumQuotaCalculations = numAnonymousQuotaCalculations;
        rgs.resourceGroupCreate(rgName, rgConfig);

        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupCreate(rgName, rgConfig));

        org.apache.pulsar.common.policies.data.ResourceGroup randomConfig =
          new org.apache.pulsar.common.policies.data.ResourceGroup();
        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupUpdate(randomRgName, randomConfig));

        rgConfig.setPublishRateInBytes(rgConfig.getPublishRateInBytes()*10);
        rgConfig.setPublishRateInMsgs(rgConfig.getPublishRateInMsgs()*10);
        rgConfig.setDispatchRateInBytes(rgConfig.getDispatchRateInBytes()/10);
        rgConfig.setDispatchRateInMsgs(rgConfig.getDispatchRateInMsgs()/10);
        rgs.resourceGroupUpdate(rgName, rgConfig);

        Assert.assertEquals(rgs.getNumResourceGroups(), 1);

        ResourceGroup retRG = rgs.resourceGroupGet(randomRgName);
        Assert.assertNull(retRG);

        retRG = rgs.resourceGroupGet(rgName);
        Assert.assertNotEquals(retRG, null);

        PerMonitoringClassFields monClassFields;
        monClassFields = retRG.monitoringClassFields[ResourceGroupMonitoringClass.Publish.ordinal()];
        Assert.assertEquals(monClassFields.configValuesPerPeriod.bytes, rgConfig.getPublishRateInBytes().longValue());
        Assert.assertEquals(monClassFields.configValuesPerPeriod.messages, rgConfig.getPublishRateInMsgs().intValue());
        monClassFields = retRG.monitoringClassFields[ResourceGroupMonitoringClass.Dispatch.ordinal()];
        Assert.assertEquals(monClassFields.configValuesPerPeriod.bytes, rgConfig.getDispatchRateInBytes().longValue());
        Assert.assertEquals(monClassFields.configValuesPerPeriod.messages, rgConfig.getDispatchRateInMsgs().intValue());

        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupDelete(randomRgName));

        Assert.assertEquals(rgs.getNumResourceGroups(), 1);

        final String SOME_RANDOM_TOPIC = "persistent://FakeTenant/FakeNameSpace/FakeTopic";
        final TopicName topic = TopicName.get(SOME_RANDOM_TOPIC);
        final String tenantName = topic.getTenant();
        final String namespaceName = topic.getNamespacePortion();
        rgs.registerTenant(rgName, tenantName);

        final NamespaceName tenantAndNamespace = NamespaceName.get(tenantName, namespaceName);
        rgs.registerNameSpace(rgName, tenantAndNamespace);

        // Delete of our valid config should throw until we unref correspondingly.
        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupDelete(rgName));

        // Attempt to report for a few rounds (simulating a fill usage with transport mgr).
        // It should say "we need to report now" every 'maxUsageReportSuppressRounds' rounds,
        // even if usage does not change.
        final ResourceUsage usage = new ResourceUsage();
        NetworkUsage nwUsage;

        boolean needToReport;
        for (int monClassIdx = 0; monClassIdx < ResourceGroupMonitoringClass.values().length; monClassIdx++) {
            ResourceGroupMonitoringClass monClass = ResourceGroupMonitoringClass.values()[monClassIdx];
            // Gross hack!
            if (monClass == ResourceGroupMonitoringClass.Publish) {
                nwUsage = usage.setPublish();
            } else {
                nwUsage = usage.setDispatch();
            }
            // We know that dummyQuotaCalc::needToReportLocalUsage() makes us report usage once every
            // maxUsageReportSuppressRounds iterations. So, if we run for maxUsageReportSuppressRounds iterations,
            // we should see needToReportLocalUsage() return true at least once.
            Set<Boolean> myBoolSet = new HashSet<>();
            for (int idx = 0; idx < ResourceGroupService.MaxUsageReportSuppressRounds; idx++) {
                needToReport = retRG.setUsageInMonitoredEntity(monClass, nwUsage);
                myBoolSet.add(needToReport);
            }
            // Expect to see at least one true and at least one false.
            Assert.assertTrue(myBoolSet.contains(true));
            Assert.assertTrue(myBoolSet.contains(false));
        }

        rgs.unRegisterTenant(rgName, tenantName);
        rgs.unRegisterNameSpace(rgName, tenantAndNamespace);

        BytesAndMessagesCount publishQuota = rgs.getPublishRateLimiters(rgName);

        // Calculated quota is synthetically set to the number of quota-calculation callbacks.
        int numQuotaCalcsDuringTest = numAnonymousQuotaCalculations - initialNumQuotaCalculations;
        if (numQuotaCalcsDuringTest == 0) {
            // Quota calculations were not done yet during this test; we expect to see the default "initial" setting.
            Assert.assertEquals(publishQuota.messages, rgConfig.getPublishRateInMsgs().intValue());
            Assert.assertEquals(publishQuota.bytes, rgConfig.getPublishRateInBytes().longValue());
        }

        // Calculate the quota synchronously to avoid waiting for a periodic call within ResourceGroupService.
        rgs.calculateQuotaForAllResourceGroups();
        publishQuota = rgs.getPublishRateLimiters(rgName);
        // The bytes/messages are (synthetically) set from numAnonymousQuotaCalculations in the above round of
        // calls, or some later round (since the periodic call to calculateQuotaForAllResourceGroups() would be
        // ongoing). So, we expect bytes/messages setting to be more than 0 and at most numAnonymousQuotaCalculations.
        Assert.assertTrue(publishQuota.messages > 0 && publishQuota.messages <= numAnonymousQuotaCalculations);
        Assert.assertTrue(publishQuota.bytes > 0 &&  publishQuota.bytes <= numAnonymousQuotaCalculations);

        rgs.resourceGroupDelete(rgName);
        Assert.assertThrows(PulsarAdminException.class, () -> rgs.getPublishRateLimiters(rgName));

        Assert.assertEquals(rgs.getNumResourceGroups(), 0);
    }

    @Test
    public void testClose() throws Exception {
        ResourceGroupService service = new ResourceGroupService(pulsar, TimeUnit.MILLISECONDS, null, null);
        service.close();
        Assert.assertTrue(service.getAggregateLocalUsagePeriodicTask().isCancelled());
        Assert.assertTrue(service.getCalculateQuotaPeriodicTask().isCancelled());
    }

    private ResourceGroupService rgs;
    int numAnonymousQuotaCalculations;

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupServiceTest.class);

    private static final int PUBLISH_INTERVAL_SECS = 500;
    private void prepareData() throws PulsarAdminException {
        this.conf.setResourceUsageTransportPublishIntervalInSecs(PUBLISH_INTERVAL_SECS);
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }
}