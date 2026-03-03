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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.PerMonitoringClassFields;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
                                                  long lastReportTimeMSecsSinceEpoch) {
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
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs) / numPerfTestIterations);

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
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs) / numPerfTestIterations);
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
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs) / numPerfTestIterations);

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

        rgConfig.setPublishRateInBytes(rgConfig.getPublishRateInBytes() * 10);
        rgConfig.setPublishRateInMsgs(rgConfig.getPublishRateInMsgs() * 10);
        rgConfig.setDispatchRateInBytes(rgConfig.getDispatchRateInBytes() / 10);
        rgConfig.setDispatchRateInMsgs(rgConfig.getDispatchRateInMsgs() / 10);
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

        final String someRandomTopic = "persistent://FakeTenant/FakeNameSpace/FakeTopic";
        final TopicName topic = TopicName.get(someRandomTopic);
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

        BytesAndMessagesCount publishQuota = rgs.getPublishRateLimiters(rgName);

        // Calculated quota is synthetically set to the number of quota-calculation callbacks.
        int numQuotaCalcsDuringTest = numAnonymousQuotaCalculations - initialNumQuotaCalculations;
        if (numQuotaCalcsDuringTest == 0) {
            // Quota calculations were not done yet during this test; we expect to see the default "initial" setting.
            Assert.assertEquals(publishQuota.messages, rgConfig.getPublishRateInMsgs().intValue());
            Assert.assertEquals(publishQuota.bytes, rgConfig.getPublishRateInBytes().longValue());
        }

        // Calculate the quota synchronously while an attachment still exists
        rgs.calculateQuotaForAllResourceGroups();
        publishQuota = rgs.getPublishRateLimiters(rgName);
        // The bytes/messages are (synthetically) set from numAnonymousQuotaCalculations in the above round of
        // calls, or some later round (since the periodic call to calculateQuotaForAllResourceGroups() would be
        // ongoing). So, we expect bytes/messages setting to be more than 0 and at most numAnonymousQuotaCalculations.
        Assert.assertTrue(publishQuota.messages > 0 && publishQuota.messages <= numAnonymousQuotaCalculations);
        Assert.assertTrue(publishQuota.bytes > 0 &&  publishQuota.bytes <= numAnonymousQuotaCalculations);

        // Now it is safe to detach. After this point the service is intentionally idle.
        rgs.unRegisterTenant(rgName, tenantName);
        rgs.unRegisterNameSpace(rgName, tenantAndNamespace);

        rgs.resourceGroupDelete(rgName);
        Assert.assertThrows(PulsarAdminException.class, () -> rgs.getPublishRateLimiters(rgName));

        Assert.assertEquals(rgs.getNumResourceGroups(), 0);
    }

    /**
     * Validates that ResourceGroupService#close() cancels scheduled tasks, clears futures and state.
     * Steps:
     * 1) Start periodic tasks by creating a resource group and attaching a namespace.
     * 2) Assert both futures are non-null (tasks are scheduled) and the schedulersRunning flag is true.
     * 3) Let try-with-resources close the service, then assert both futures are null, schedulersRunning is false,
     *    and the resource group map is cleared.
     */
    @Test(timeOut = 60000)
    public void testClose() throws Exception {
        final String rg = "rg-close";
        final NamespaceName ns = NamespaceName.get("t-close/ns-close");

        // Create the service once so we can assert state after the try-with-resources closes it.
        ResourceGroupService service = createResourceGroupService();
        try (ResourceGroupService ignored = service) {
            // Start the periodic tasks: create RG and attach a namespace.
            service.resourceGroupCreate(rg, new org.apache.pulsar.common.policies.data.ResourceGroup());
            service.registerNameSpace(rg, ns);

            // Ensure tasks are started
            Assert.assertNotNull(service.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be scheduled.");
            Assert.assertNotNull(service.getCalculateQuotaPeriodicTask(),
                    "Quota task should be scheduled.");
            Assert.assertTrue(service.isSchedulersRunning(),
                    "SchedulersRunning flag should be true when tasks are active.");

            // Do not call service.close() here. The try-with-resources will close it and ensure cleanup.
        }

        // After the try-with-resources block, service.close() has been invoked automatically.
        // Postconditions: futures cleared to null, internal state cleared, flag off.
        Assert.assertNull(service.getAggregateLocalUsagePeriodicTask(),
                "Aggregate task future must be null after close().");
        Assert.assertNull(service.getCalculateQuotaPeriodicTask(),
                "Quota task future must be null after close().");
        Assert.assertEquals(service.getNumResourceGroups(), 0,
                "Resource group map should be cleared by close().");
        Assert.assertFalse(service.isSchedulersRunning(),
                "SchedulersRunning flag should be false after close().");
    }

    private ResourceGroupService rgs;
    int numAnonymousQuotaCalculations;

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupServiceTest.class);

    private static final int PUBLISH_INTERVAL_SECS = 500;
    private void prepareData() throws PulsarAdminException {
        this.conf.setResourceUsageTransportPublishIntervalInSecs(PUBLISH_INTERVAL_SECS);
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }
    /**
     * Helper method to create a fresh ResourceGroupService instance for testing.
     * Each test should create its own instance to ensure isolation.
     */
    private ResourceGroupService createResourceGroupService() {
        return new ResourceGroupService(pulsar);
    }

    /**
     * Validates the lazy scheduling lifecycle and deterministic rescheduling of ResourceGroupService.
     * Asserts that:
     * 1) On cold start, and after creating a Resource Group (RG) without any attachments,
     * no periodic tasks are scheduled.
     * 2) Registering the first attachment (tenant or namespace) starts both periodic tasks.
     * 3) Updating the publish interval causes rescheduling
     *    - calling aggregateResourceGroupLocalUsages() reschedules only the aggregation task;
     *    - calling calculateQuotaForAllResourceGroups() reschedules only the quota-calculation task.
     * 4) When the last attachment is unregistered (i.e., no tenants or namespaces remain attached to any RG),
     *    both periodic tasks are cancelled and their ScheduledFuture fields are cleared.
     */

    @Test(timeOut = 60000)
    public void testLazyStartStopAndReschedule() throws Exception {
        final String rgName = "rg-lazy";
        final NamespaceName ns = NamespaceName.get("t-lazy/ns-lazy");
        final int oldInterval = this.conf.getResourceUsageTransportPublishIntervalInSecs();

        try (ResourceGroupService rgs = createResourceGroupService()) {
            // Cold start: nothing scheduled
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be null on cold start.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should be null on cold start.");

            // Create a resource group but do not attach yet. There should still be nothing scheduled.
            rgs.resourceGroupCreate(rgName, new org.apache.pulsar.common.policies.data.ResourceGroup());
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should remain null without attachments.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should remain null without attachments.");

            // Attach a namespace. Both schedulers must start.
            rgs.registerNameSpace(rgName, ns);
            Assert.assertNotNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should start on first attachment.");
            Assert.assertNotNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should start on first attachment.");
            Assert.assertFalse(rgs.getAggregateLocalUsagePeriodicTask().isCancelled(),
                    "Aggregate task should be running.");
            Assert.assertFalse(rgs.getCalculateQuotaPeriodicTask().isCancelled(),
                    "Quota task should be running.");

            // Capture current scheduled futures
            java.util.concurrent.ScheduledFuture<?> oldAgg = rgs.getAggregateLocalUsagePeriodicTask();
            java.util.concurrent.ScheduledFuture<?> oldCalc = rgs.getCalculateQuotaPeriodicTask();

            // Change publish interval dynamically
            int newPeriod = oldInterval + 1;
            this.conf.setResourceUsageTransportPublishIntervalInSecs(newPeriod);

            // Trigger aggregate reschedule
            rgs.aggregateResourceGroupLocalUsages();
            java.util.concurrent.ScheduledFuture<?> midAgg = rgs.getAggregateLocalUsagePeriodicTask();
            java.util.concurrent.ScheduledFuture<?> midCalc = rgs.getCalculateQuotaPeriodicTask();

            Assert.assertNotSame(midAgg, oldAgg, "Aggregate task should be rescheduled with a new future.");
            Assert.assertTrue(oldAgg.isCancelled(), "Old aggregate task should be cancelled on reschedule.");
            Assert.assertSame(midCalc, oldCalc, "Quota task should not be rescheduled by aggregate path.");
            Assert.assertFalse(oldCalc.isCancelled(),
                    "Old quota task should still be active before its own reschedule.");
            Assert.assertFalse(midAgg.isCancelled(), "New aggregate task should be active.");

            // Now trigger calculate reschedule
            rgs.calculateQuotaForAllResourceGroups();
            java.util.concurrent.ScheduledFuture<?> newAgg = rgs.getAggregateLocalUsagePeriodicTask();
            java.util.concurrent.ScheduledFuture<?> newCalc = rgs.getCalculateQuotaPeriodicTask();

            Assert.assertSame(newAgg, midAgg,
                    "Aggregate task was already rescheduled. Future should remain the same.");
            Assert.assertNotSame(newCalc, oldCalc, "Quota task should be rescheduled with a new future.");
            Assert.assertTrue(oldCalc.isCancelled(), "Old quota task should be cancelled on its reschedule.");
            Assert.assertFalse(newCalc.isCancelled(), "New quota task should be active.");

            // Detach the last attachment. Schedulers must stop and futures should be cleared.
            rgs.unRegisterNameSpace(rgName, ns);
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be cleared after last detach.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should be cleared after last detach.");

            // Cleanup resource group
            rgs.resourceGroupDelete(rgName);
            Assert.assertEquals(rgs.getNumResourceGroups(), 0, "Resource group should be deleted.");
        } finally {
            this.conf.setResourceUsageTransportPublishIntervalInSecs(oldInterval);
        }
    }

    /**
     * Verifies that creating a ResourceGroup without tenant or namespace attachments
     * does NOT trigger scheduler initialization. This ensures the lazy-start optimization
     * correctly avoids unnecessary overhead when ResourceGroups are configured but unused.
     */
    @Test(timeOut = 60000)
    public void testNoStartOnRGCreateOnly() throws Exception {
        final String rg = "rg-create-only";
        final int oldInterval = this.conf.getResourceUsageTransportPublishIntervalInSecs();

        try (ResourceGroupService rgs = createResourceGroupService()) {
            // No tasks at cold start
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be null at cold start.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(), "Quota task should be null at cold start.");

            // Create resource group without attachments. There should still be nothing scheduled.
            rgs.resourceGroupCreate(rg, new org.apache.pulsar.common.policies.data.ResourceGroup());
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should remain null without attachments.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should remain null without attachments.");

            // Calling periodic methods directly must not create schedulers
            rgs.aggregateResourceGroupLocalUsages();
            rgs.calculateQuotaForAllResourceGroups();
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should remain null after direct calls.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should remain null after direct calls.");

            // Dynamic config while stopped must not trigger scheduling
            this.conf.setResourceUsageTransportPublishIntervalInSecs(oldInterval + 7);
            rgs.aggregateResourceGroupLocalUsages();
            rgs.calculateQuotaForAllResourceGroups();
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should remain null after config change while stopped.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should remain null after config change while stopped.");

            // Cleanup resource group
            rgs.resourceGroupDelete(rg);
            Assert.assertEquals(rgs.getNumResourceGroups(), 0, "Resource group should be deleted.");
        } finally {
            this.conf.setResourceUsageTransportPublishIntervalInSecs(oldInterval);
        }
    }

    /**
     * Validates that attaching a tenant (without namespace attachment) to a ResourceGroup
     * triggers scheduler initialization, and that detaching the last tenant stops the schedulers.
     */
    @Test(timeOut = 60000)
    public void testStartOnTenantAttachment() throws Exception {
        final String rg = "rg-tenant-only";
        final String tenant = "t-attach";

        try (ResourceGroupService rgs = createResourceGroupService()) {
            rgs.resourceGroupCreate(rg, new org.apache.pulsar.common.policies.data.ResourceGroup());
            rgs.registerTenant(rg, tenant);

            Assert.assertNotNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should start on first tenant attachment.");
            Assert.assertNotNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should start on first tenant attachment.");
            Assert.assertFalse(rgs.getAggregateLocalUsagePeriodicTask().isCancelled(),
                    "Aggregate task should be running.");
            Assert.assertFalse(rgs.getCalculateQuotaPeriodicTask().isCancelled(),
                    "Quota task should be running.");

            // Detach and ensure schedulers stop
            rgs.unRegisterTenant(rg, tenant);
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be cleared after last detach.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should be cleared after last detach.");

            rgs.resourceGroupDelete(rg);
            Assert.assertEquals(rgs.getNumResourceGroups(), 0, "Resource group should be deleted.");
        }
    }

    /**
     * Tests scheduler lifecycle when a ResourceGroup has both tenant and namespace attachments.
     * Verifies that schedulers remain active as long as ANY attachment exists, and only stop
     * when the last attachment (tenant or namespace) is removed.
     */
    @Test(timeOut = 60000)
    public void testStopOnLastDetachWithMixedRefs() throws Exception {
        final String rg = "rg-mixed";
        final String tenant = "t-mixed";
        final NamespaceName ns = NamespaceName.get("t-mixed/ns1");

        try (ResourceGroupService rgs = createResourceGroupService()) {
            rgs.resourceGroupCreate(rg, new org.apache.pulsar.common.policies.data.ResourceGroup());

            // Attach both a tenant and a namespace
            rgs.registerTenant(rg, tenant);
            rgs.registerNameSpace(rg, ns);

            Assert.assertNotNull(rgs.getAggregateLocalUsagePeriodicTask(), "Aggregate task should be started.");
            Assert.assertNotNull(rgs.getCalculateQuotaPeriodicTask(), "Quota task should be started.");

            // Remove one reference. Tasks should still be present.
            rgs.unRegisterTenant(rg, tenant);
            Assert.assertNotNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should remain with remaining namespace attachment.");
            Assert.assertNotNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should remain with remaining namespace attachment.");

            // Remove last reference. Tasks should stop.
            rgs.unRegisterNameSpace(rg, ns);
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(),
                    "Aggregate task should be cleared after last detach.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(),
                    "Quota task should be cleared after last detach.");

            rgs.resourceGroupDelete(rg);
            Assert.assertEquals(rgs.getNumResourceGroups(), 0, "Resource group should be deleted.");
        }
    }

    /**
     * Ensures that dynamic configuration changes to the publish interval do NOT cause
     * scheduler initialization when no attachments exist. This validates the guard logic
     * that prevents spurious rescheduling attempts on stopped schedulers.
     */
    @Test(timeOut = 60000)
    public void testNoRescheduleWhenStopped() throws Exception {
        final int oldInterval = this.conf.getResourceUsageTransportPublishIntervalInSecs();

        try (ResourceGroupService rgs = createResourceGroupService()) {
            // Ensure stopped state
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(), "Aggregate task should be null.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(), "Quota task should be null.");

            // Change interval while stopped
            this.conf.setResourceUsageTransportPublishIntervalInSecs(oldInterval + 13);

            // Call both periodic methods directly. Futures must remain null.
            rgs.aggregateResourceGroupLocalUsages();
            rgs.calculateQuotaForAllResourceGroups();
            Assert.assertNull(rgs.getAggregateLocalUsagePeriodicTask(), "Aggregate task should remain null.");
            Assert.assertNull(rgs.getCalculateQuotaPeriodicTask(), "Quota task should remain null.");
        } finally {
            this.conf.setResourceUsageTransportPublishIntervalInSecs(oldInterval);
        }
    }


}