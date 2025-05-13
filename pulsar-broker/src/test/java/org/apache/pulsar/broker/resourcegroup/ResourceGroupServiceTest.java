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

import static org.mockito.Mockito.mock;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy.Expiration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.PerBrokerUsageStats;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.PerMonitoringClassFields;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.resource.usage.ReplicatorUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
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
                                                  long lastReportTimeMSecsSinceEpoch)
            {
                final int maxSuppressRounds = conf.getResourceUsageMaxUsageReportSuppressRounds();
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
        final int numPerfTestIterations = 1_000;
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
                rg.incrementLocalUsageStats(monClass, stats,
                        monClass.equals(ResourceGroupMonitoringClass.ReplicationDispatch) ? "r2" : null);
            }
        }
        mSecsEnd = System.currentTimeMillis();
        diffMsecs = mSecsEnd - mSecsStart;
        log.info("{} iterations of incrementLocalUsageStats on retRG in {} msecs ({} usecs for each)",
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs)/numPerfTestIterations);

        // Going through the resource-group service
        final TopicName topicName = TopicName.get("SomeTenant/SomeNameSpace/my-topic");
        rgs.registerTopic(rgName, topicName);
        final String tenantName = topicName.getTenant();
        final String namespaceName = topicName.getNamespace();
        rgs.registerTenant(rgName, tenantName);
        final NamespaceName tenantAndNamespaceName = topicName.getNamespaceObject();
        rgs.registerNameSpace(rgName, tenantAndNamespaceName);
        mSecsStart = System.currentTimeMillis();
        for (int ix = 0; ix < numPerfTestIterations; ix++) {
            for (int monClassIdx = 0; monClassIdx < ResourceGroupMonitoringClass.values().length; monClassIdx++) {
                monClass = ResourceGroupMonitoringClass.values()[monClassIdx];
                rgs.incrementUsage(tenantName, namespaceName, topicName.toString(), monClass, stats, monClass.equals(ResourceGroupMonitoringClass.ReplicationDispatch) ? "r2" : null);
            }
        }
        mSecsEnd = System.currentTimeMillis();
        diffMsecs = mSecsEnd - mSecsStart;
        log.info("{} iterations of incrementUsage on RGS in {} msecs ({} usecs for each)",
                numPerfTestIterations, diffMsecs, (1000 * (float) diffMsecs)/numPerfTestIterations);
        rgs.unRegisterTenant(rgName, tenantName);
        rgs.unRegisterNameSpace(rgName, tenantAndNamespaceName);
        rgs.unRegisterTopic(topicName);

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
    public void testReplicatorResourceGroupOps() throws PulsarAdminException {
        String rgName = "testRG-" + System.currentTimeMillis();
        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        rgConfig.setReplicationDispatchRateInBytes(2000L);
        rgConfig.setReplicationDispatchRateInMsgs(400L);

        rgs.resourceGroupCreate(rgName, rgConfig);
        ResourceGroup retRG = rgs.resourceGroupGet(rgName);
        Assert.assertNotEquals(retRG, null);

        PerMonitoringClassFields r1 = retRG.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch, "r1");
        Assert.assertEquals(r1.configValuesPerPeriod.bytes,
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r1.configValuesPerPeriod.messages,
                rgConfig.getReplicationDispatchRateInMsgs().intValue());

        AtomicReference<ResourceGroupDispatchLimiter> r1Limiter = new AtomicReference<>();
        retRG.registerReplicatorDispatchRateLimiter("r1", r1Limiter::set);
        AtomicReference<ResourceGroupDispatchLimiter> r2Limiter = new AtomicReference<>();
        retRG.registerReplicatorDispatchRateLimiter("r2", r2Limiter::set);

        Assert.assertEquals(r1Limiter.get(), r2Limiter.get());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());

        // Update the default rate limitation.
        rgConfig.setReplicationDispatchRateInBytes(rgConfig.getReplicationDispatchRateInBytes() * 10);
        rgConfig.setReplicationDispatchRateInMsgs(rgConfig.getReplicationDispatchRateInMsgs() * 10);
        rgs.resourceGroupUpdate(rgName, rgConfig);

        Assert.assertEquals(r1Limiter.get(), r2Limiter.get());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());

        // Set up the specific rate limitation based on the remote cluster.
        Map<String, DispatchRate> replicatorLimiterMap = new ConcurrentHashMap<>();
        // r1
        DispatchRate newR1Limiter =
                DispatchRate.builder().dispatchThrottlingRateInMsg(1024).dispatchThrottlingRateInByte(200).build();
        replicatorLimiterMap.put(retRG.getReplicatorDispatchRateLimiterKey("r1"), newR1Limiter);
        rgConfig.setReplicatorDispatchRate(replicatorLimiterMap);
        rgs.resourceGroupUpdate(rgName, rgConfig);

        Assert.assertNotEquals(r1Limiter.get(), r2Limiter.get());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(), newR1Limiter.getDispatchThrottlingRateInByte());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(), newR1Limiter.getDispatchThrottlingRateInMsg());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());

        // r2
        DispatchRate newR2Limiter =
                DispatchRate.builder().dispatchThrottlingRateInMsg(2048).dispatchThrottlingRateInByte(400).build();
        replicatorLimiterMap.put(retRG.getReplicatorDispatchRateLimiterKey("r2"), newR2Limiter);
        rgs.resourceGroupUpdate(rgName, rgConfig);

        Assert.assertNotEquals(r1Limiter.get(), r2Limiter.get());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(), newR1Limiter.getDispatchThrottlingRateInByte());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(), newR1Limiter.getDispatchThrottlingRateInMsg());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(), newR2Limiter.getDispatchThrottlingRateInByte());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(), newR2Limiter.getDispatchThrottlingRateInMsg());

        // remove r1
        replicatorLimiterMap.remove(retRG.getReplicatorDispatchRateLimiterKey("r1"));
        rgs.resourceGroupUpdate(rgName, rgConfig);
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(), newR2Limiter.getDispatchThrottlingRateInByte());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(), newR2Limiter.getDispatchThrottlingRateInMsg());

        // remove r2
        replicatorLimiterMap.remove(retRG.getReplicatorDispatchRateLimiterKey("r2"));
        rgs.resourceGroupUpdate(rgName, rgConfig);
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(),
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(),
                rgConfig.getReplicationDispatchRateInMsgs().longValue());

        rgs.resourceGroupDelete(rgName);

        // Check the rate limiters are removed.
        Assert.assertNull(r1Limiter.get());
        Assert.assertNull(r2Limiter.get());
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
        rgConfig.setReplicationDispatchRateInBytes(2000L);
        rgConfig.setReplicationDispatchRateInMsgs(400L);

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
        rgConfig.setReplicationDispatchRateInBytes(rgConfig.getReplicationDispatchRateInBytes()/10);
        rgConfig.setReplicationDispatchRateInMsgs(rgConfig.getReplicationDispatchRateInMsgs()/10);
        rgs.resourceGroupUpdate(rgName, rgConfig);

        Assert.assertEquals(rgs.getNumResourceGroups(), 1);

        Assert.assertNull(rgs.resourceGroupGet(randomRgName));

        ResourceGroup retRG = rgs.resourceGroupGet(rgName);
        Assert.assertNotEquals(retRG, null);

        PerMonitoringClassFields monClassFields;
        monClassFields = retRG.getMonitoredEntity(ResourceGroupMonitoringClass.Publish, null);
        Assert.assertEquals(monClassFields.configValuesPerPeriod.bytes, rgConfig.getPublishRateInBytes().longValue());
        Assert.assertEquals(monClassFields.configValuesPerPeriod.messages, rgConfig.getPublishRateInMsgs().intValue());
        monClassFields = retRG.getMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, null);
        Assert.assertEquals(monClassFields.configValuesPerPeriod.bytes, rgConfig.getDispatchRateInBytes().longValue());
        Assert.assertEquals(monClassFields.configValuesPerPeriod.messages, rgConfig.getDispatchRateInMsgs().intValue());
        monClassFields = retRG.getMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch, "r1");
        Assert.assertEquals(monClassFields.configValuesPerPeriod.bytes,
                rgConfig.getReplicationDispatchRateInBytes().longValue());
        Assert.assertEquals(monClassFields.configValuesPerPeriod.messages,
                rgConfig.getReplicationDispatchRateInMsgs().intValue());

        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupDelete(randomRgName));

        Assert.assertEquals(rgs.getNumResourceGroups(), 1);

        final String SOME_RANDOM_TOPIC = "persistent://FakeTenant/FakeNameSpace/FakeTopic";
        final TopicName topic = TopicName.get(SOME_RANDOM_TOPIC);
        final String tenantName = topic.getTenant();
        final String namespaceName = topic.getNamespacePortion();
        rgs.registerTenant(rgName, tenantName);

        final NamespaceName tenantAndNamespace = NamespaceName.get(tenantName, namespaceName);
        rgs.registerNameSpace(rgName, tenantAndNamespace);
        rgs.registerTopic(rgName, topic);

        // Delete of our valid config should throw until we unref correspondingly.
        Assert.assertThrows(PulsarAdminException.class, () -> rgs.resourceGroupDelete(rgName));

        // Attempt to report for a few rounds (simulating a fill usage with transport mgr).
        // It should say "we need to report now" every 'maxUsageReportSuppressRounds' rounds,
        // even if usage does not change.
        retRG.getMonitoringClassFieldsMap().forEach((k, v) -> {
            List<ResourceUsage> usageList = new ArrayList<>();
            for (int idx = 0; idx < conf.getResourceUsageMaxUsageReportSuppressRounds(); idx++) {
                ResourceUsage usage = new ResourceUsage();
                usageList.add(usage);
                retRG.setUsageInMonitoredEntity(usage);
            }
            // Expect to see at least one true and at least one false.
            Assert.assertTrue(usageList.stream()
                    .anyMatch(n -> n.hasPublish() || n.hasDispatch() || n.getReplicatorsCount() != 0));
            Assert.assertTrue(usageList.stream()
                    .anyMatch(n -> (!n.hasPublish()) && (!n.hasDispatch()) && (n.getReplicatorsCount() == 0)));
        });

        rgs.unRegisterTenant(rgName, tenantName);
        rgs.unRegisterNameSpace(rgName, tenantAndNamespace);
        rgs.unRegisterTopic(topic);

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

        Assert.assertEquals(rgs.getTopicConsumeStats().estimatedSize(), 0);
        Assert.assertEquals(rgs.getTopicProduceStats().estimatedSize(), 0);
        Assert.assertEquals(rgs.getReplicationDispatchStats().estimatedSize(), 0);
        Assert.assertEquals(rgs.getTopicToReplicatorsMap().size(), 0);
    }

    @Test
    public void testCleanupStatsWhenUnRegisterTopic()
            throws PulsarAdminException {
        String tenantName = UUID.randomUUID().toString();
        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        final String rgName = UUID.randomUUID().toString();
        rgConfig.setPublishRateInBytes(15000L);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(40000L);
        rgConfig.setDispatchRateInMsgs(500);
        rgConfig.setReplicationDispatchRateInBytes(2000L);
        rgConfig.setReplicationDispatchRateInMsgs(400L);

        rgs.resourceGroupCreate(rgName, rgConfig);
        String nsName = tenantName + "/" + UUID.randomUUID();
        TopicName topicName = TopicName.get(nsName + "/" + UUID.randomUUID());
        String topic = topicName.toString();

        rgs.registerTopic(rgName, topicName);

        // Simulate replicator
        rgs.updateStatsWithDiff(topic, "remote-cluster", tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.ReplicationDispatch);
        rgs.updateStatsWithDiff(topic, null, tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.Publish);
        rgs.updateStatsWithDiff(topic, null, tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.Dispatch);
        Assert.assertEquals(rgs.getTopicProduceStats().asMap().size(), 1);
        Assert.assertEquals(rgs.getTopicConsumeStats().asMap().size(), 1);
        Assert.assertEquals(rgs.getReplicationDispatchStats().asMap().size(), 1);
        Assert.assertEquals(rgs.getTopicToReplicatorsMap().size(), 1);
        Set<String> replicators = rgs.getTopicToReplicatorsMap().get(rgs.getTopicToReplicatorsMap().keys().nextElement());
        Assert.assertEquals(replicators.size(), 1);

        rgs.unRegisterTopic(TopicName.get(topic));

        Assert.assertEquals(rgs.getTopicProduceStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getTopicConsumeStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getReplicationDispatchStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getReplicationDispatchStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getTopicToReplicatorsMap().size(), 0);

        rgs.resourceGroupDelete(rgName);
    }

    @Test
    public void testCleanupStatsWhenUnRegisterNamespace()
            throws PulsarAdminException {
        String tenantName = UUID.randomUUID().toString();
        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        final String rgName = UUID.randomUUID().toString();
        rgConfig.setPublishRateInBytes(15000L);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(40000L);
        rgConfig.setDispatchRateInMsgs(500);
        rgConfig.setReplicationDispatchRateInBytes(2000L);
        rgConfig.setReplicationDispatchRateInMsgs(400L);

        rgs.resourceGroupCreate(rgName, rgConfig);
        String nsName = tenantName + "/" + UUID.randomUUID();
        TopicName topicName = TopicName.get(nsName + "/" + UUID.randomUUID());
        String topic = topicName.toString();

        rgs.registerNameSpace(rgName, topicName.getNamespaceObject());

        // Simulate replicator
        rgs.updateStatsWithDiff(topic, "remote-cluster", tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.ReplicationDispatch);
        rgs.updateStatsWithDiff(topic, null, tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.Publish);
        rgs.updateStatsWithDiff(topic, null, tenantName, nsName, 1, 1,
                ResourceGroupMonitoringClass.Dispatch);
        Assert.assertEquals(rgs.getTopicProduceStats().asMap().size(), 1);
        Assert.assertEquals(rgs.getTopicConsumeStats().asMap().size(), 1);
        Assert.assertEquals(rgs.getReplicationDispatchStats().asMap().size(), 1);
        Set<String> replicators = rgs.getTopicToReplicatorsMap().get(rgs.getTopicToReplicatorsMap().keys().nextElement());
        Assert.assertEquals(replicators.size(), 1);

        rgs.unRegisterNameSpace(rgName, topicName.getNamespaceObject());

        Assert.assertEquals(rgs.getTopicProduceStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getTopicConsumeStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getReplicationDispatchStats().asMap().size(), 0);
        Assert.assertEquals(rgs.getTopicToReplicatorsMap().size(), 0);

        rgs.resourceGroupDelete(rgName);
    }

    @Test
    public void testClose() throws Exception {
        ResourceGroupService service = new ResourceGroupService(pulsar, TimeUnit.MILLISECONDS, null, null);
        service.close();
        Assert.assertTrue(service.getAggregateLocalUsagePeriodicTask().isCancelled());
        Assert.assertTrue(service.getCalculateQuotaPeriodicTask().isCancelled());
    }

    private void assertTopicStatsCache(Cache<String, BytesAndMessagesCount> cache, long durationMS) {
        Optional<Expiration<String, BytesAndMessagesCount>> expirationOptional =
                cache.policy().expireAfterAccess();
        Assert.assertTrue(expirationOptional.isPresent());
        Expiration<String, BytesAndMessagesCount> expiration = expirationOptional.get();
        Assert.assertEquals(expiration.getExpiresAfter().toMillis(), durationMS);
    }

    @Test
    public void testTopicStatsCache() {
        long ms = 2_000;
        Cache<String, BytesAndMessagesCount> cache =
                pulsar.getResourceGroupServiceManager().newStatsCache(TimeUnit.MILLISECONDS.toMillis(ms));
        String key = "topic-1";
        BytesAndMessagesCount value = new BytesAndMessagesCount();
        cache.put(key, value);
        Assert.assertEquals(cache.getIfPresent(key), value);
        Awaitility.await().pollDelay(ms + 200 , TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(cache.getIfPresent(key));
        });

        long expMS =
                TimeUnit.SECONDS.toMillis(pulsar.getConfiguration().getResourceUsageTransportPublishIntervalInSecs())
                        * 2;
        assertTopicStatsCache(pulsar.getResourceGroupServiceManager().getTopicConsumeStats(), expMS);
        assertTopicStatsCache(pulsar.getResourceGroupServiceManager().getTopicProduceStats(), expMS);
        assertTopicStatsCache(pulsar.getResourceGroupServiceManager().getReplicationDispatchStats(), expMS);
    }

    @Test
    public void testBrokerStatsCache() throws PulsarAdminException {
        long ms = 2_000;
        String key = "broker-1";
        PerBrokerUsageStats value = new PerBrokerUsageStats();
        PerMonitoringClassFields perMonitoringClassFields = PerMonitoringClassFields.create(ms);
        Cache<String, PerBrokerUsageStats> usageFromOtherBrokers = perMonitoringClassFields.usageFromOtherBrokers;
        usageFromOtherBrokers.put(key, value);
        Assert.assertEquals(usageFromOtherBrokers.getIfPresent(key), value);
        Awaitility.await().atMost(ms + 500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(usageFromOtherBrokers.getIfPresent(key));
        });

        org.apache.pulsar.common.policies.data.ResourceGroup rgConfig =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        final String rgName = UUID.randomUUID().toString();
        rgConfig.setPublishRateInBytes(15000L);
        rgConfig.setPublishRateInMsgs(100);
        rgConfig.setDispatchRateInBytes(40000L);
        rgConfig.setDispatchRateInMsgs(500);
        rgConfig.setReplicationDispatchRateInBytes(2000L);
        rgConfig.setReplicationDispatchRateInMsgs(400L);

        ResourceGroupService resourceGroupServiceManager = pulsar.getResourceGroupServiceManager();
        resourceGroupServiceManager.resourceGroupCreate(rgName, rgConfig);
        ResourceGroup resourceGroup = resourceGroupServiceManager.resourceGroupGet(rgName);
        PerMonitoringClassFields publishMonitoredEntity =
                resourceGroup.getMonitoredEntity(ResourceGroupMonitoringClass.Publish, null);
        Cache<String, PerBrokerUsageStats> cache = publishMonitoredEntity.usageFromOtherBrokers;
        Optional<Expiration<String, PerBrokerUsageStats>> expirationOptional =
                cache.policy().expireAfterWrite();
        Assert.assertTrue(expirationOptional.isPresent());
        Expiration<String, PerBrokerUsageStats> brokerUsageStatsExpiration = expirationOptional.get();

        long statsDuration =
                TimeUnit.SECONDS.toMillis(pulsar.getConfiguration().getResourceUsageTransportPublishIntervalInSecs())
                        * conf.getResourceUsageMaxUsageReportSuppressRounds() * 2;
        Assert.assertEquals(brokerUsageStatsExpiration.getExpiresAfter().toMillis(), statsDuration);
    }

    protected String getReplicatorDispatchRateLimiterKey(String remoteCluster) {
        return DispatchRateLimiter.getReplicatorDispatchRateKey(pulsar.getConfiguration().getClusterName(),
                remoteCluster);
    }

    @Test
    public void testQuotaCalculation() throws PulsarAdminException {
        // Mocks and setup
        ResourceUsageTransportManager resourceUsageTransportManager = mock(ResourceUsageTransportManager.class);
        ResourceGroupService resourceGroupService = new ResourceGroupService(
                pulsar, TimeUnit.HOURS, resourceUsageTransportManager,
                new ResourceQuotaCalculatorImpl(pulsar)
        );

        // Setup: Resource Group Configuration
        String rg1Name = UUID.randomUUID().toString();
        org.apache.pulsar.common.policies.data.ResourceGroup rg1Config =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        rg1Config.setPublishRateInBytes(100L);
        rg1Config.setPublishRateInMsgs(200);
        rg1Config.setDispatchRateInBytes(40000L);
        rg1Config.setDispatchRateInMsgs(500);
        rg1Config.setReplicationDispatchRateInBytes(2000L);
        rg1Config.setReplicationDispatchRateInMsgs(400L);
        Map<String, DispatchRate> replicatorDispatchRateMap = new ConcurrentHashMap<>();
        DispatchRate r1 = DispatchRate.builder().dispatchThrottlingRateInMsg(300).dispatchThrottlingRateInByte(150)
                .ratePeriodInSecond(1).build();
        replicatorDispatchRateMap.put(getReplicatorDispatchRateLimiterKey("r1"), r1);
        rg1Config.setReplicatorDispatchRate(replicatorDispatchRateMap);

        resourceGroupService.resourceGroupCreate(rg1Name, rg1Config);
        ResourceGroup rg1Ref = resourceGroupService.resourceGroupGet(rg1Name);

        // Verify initial publish limiter values
        ResourceGroupPublishLimiter publishLimiter = rg1Ref.getResourceGroupPublishLimiter();
        Assert.assertEquals(publishLimiter.getResourceGroupPublishValues().bytes, 100L);
        Assert.assertEquals(publishLimiter.getResourceGroupPublishValues().messages, 200);

        // Setup replicator dispatch limiters
        AtomicReference<ResourceGroupDispatchLimiter> r1Limiter = new AtomicReference<>();
        AtomicReference<ResourceGroupDispatchLimiter> r2Limiter = new AtomicReference<>();
        rg1Ref.registerReplicatorDispatchRateLimiter("r1", r1Limiter::set);
        rg1Ref.registerReplicatorDispatchRateLimiter("r2", r2Limiter::set);

        // r1 uses the specific rate limiter
        // r2 uses the default rate limiter
        Assert.assertNotEquals(r1Limiter.get(), r2Limiter.get());
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(), 300L);
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(), 150L);
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(), 400L);
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(), 2000L);

        // Simulate local usage
        ResourceUsage localUsage = new ResourceUsage();
        // Local dispatch usage
        localUsage.setDispatch().setBytesPerPeriod(100).setMessagesPerPeriod(200);
        // Local publish usage
        localUsage.setPublish().setBytesPerPeriod(50).setMessagesPerPeriod(150);
        // Local replication usage for r1
        ReplicatorUsage localR1 = localUsage.addReplicator();
        localR1.setRemoteCluster("r1");
        localR1.setNetworkUsage().setBytesPerPeriod(75).setMessagesPerPeriod(100);
        // Local replication usage for r2
        ReplicatorUsage localR2 = localUsage.addReplicator();
        localR2.setRemoteCluster("r2");
        localR2.setNetworkUsage().setBytesPerPeriod(1400).setMessagesPerPeriod(90);

        // Simulate remote usage
        ResourceUsage remoteUsage = new ResourceUsage();
        // Remote dispatch usage
        remoteUsage.setDispatch().setBytesPerPeriod(500).setMessagesPerPeriod(800);
        // Remote publish usage
        remoteUsage.setPublish().setBytesPerPeriod(250).setMessagesPerPeriod(600);
        // Remote replication usage for r1
        ReplicatorUsage remoteR1 = remoteUsage.addReplicator();
        remoteR1.setRemoteCluster("r1");
        remoteR1.setNetworkUsage().setBytesPerPeriod(50).setMessagesPerPeriod(20);
        // Remote replication usage for r2
        ReplicatorUsage remoteR2 = remoteUsage.addReplicator();
        remoteR2.setRemoteCluster("r2");
        remoteR2.setNetworkUsage().setBytesPerPeriod(400).setMessagesPerPeriod(280);

        // Report usages
        rg1Ref.getUsageFromMonitoredEntity(remoteUsage, "pulsar://broker-2:6650");
        rg1Ref.getUsageFromMonitoredEntity(localUsage, pulsar.getBrokerServiceUrl());

        // Update the local quota
        resourceGroupService.calculateQuotaByMonClass(rg1Name, rg1Ref, ResourceGroupMonitoringClass.Publish);
        resourceGroupService.calculateQuotaByMonClass(rg1Name, rg1Ref,
                ResourceGroupMonitoringClass.ReplicationDispatch);

        // b100, m200
        // publish:
        // Global limits: messages = 200, bytes = 100
        // Usage:
        //  - Local: messages = 150, bytes = 50
        // - Remote: messages = 600, bytes = 250
        // Quota:
        // - Messages: 150 / (150 + 600) * 200 ≈ 40
        // - Bytes:    50 / (50 + 250) * 100 ≈ 16
        publishLimiter = rg1Ref.getResourceGroupPublishLimiter();
        Assert.assertEquals(publishLimiter.getResourceGroupPublishValues().bytes, 16);
        Assert.assertEquals(publishLimiter.getResourceGroupPublishValues().messages, 40);

        // r1:
        // Global limits: messages = 300, bytes = 150
        // Usage:
        //   - Local: messages = 100, bytes = 75
        //   - Remote: messages = 20, bytes = 50
        // Quota:
        //   - Messages: 100 / (100 + 20) * 300 ≈ 250
        //   - Bytes:    75 / (75 + 50) * 150 ≈ 90
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnByte(), 90);
        Assert.assertEquals(r1Limiter.get().getDispatchRateOnMsg(), 250);

        // r2:
        // Global limits: messages = 400, bytes = 2000
        // Usage:
        //   - Local: messages = 90, bytes = 1400
        //   - Remote: messages = 280, bytes = 400
        // Quota:
        //   - Messages: 90 / (90 + 280) * 400 ≈ 97
        //   - Bytes:    1400 / (1400 + 400) * 2000 ≈ 1555
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnByte(), 1555);
        Assert.assertEquals(r2Limiter.get().getDispatchRateOnMsg(), 97);
    }

    private ResourceGroupService rgs;
    int numAnonymousQuotaCalculations;

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupServiceTest.class);

    private static final int PUBLISH_INTERVAL_SECS = 500;
    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }
}