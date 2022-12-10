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
package org.apache.pulsar.broker.loadbalance.extensions.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;


@Test(groups = "broker")
public class TransferShedderTest {

    public LoadManagerContext setupContext(){
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerExtentionsTransferShedderDebugModeEnabled(true);

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(2));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(4));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(6));
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(80));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(90));

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("bundleA", 1, 1));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("bundleB", 3, 1));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("bundleC", 4, 2));
        topBundlesLoadDataStore.pushAsync("broker4", getTopBundlesLoad("bundleD", 20, 60));
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("bundleE", 70, 20));
        return ctx;
    }

    public LoadManagerContext setupContext(int clusterSize) {
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerExtentionsTransferShedderDebugModeEnabled(true);

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        Random rand = new Random();
        for (int i = 0; i < clusterSize; i++) {
            int brokerLoad = rand.nextInt(100);
            brokerLoadDataStore.pushAsync("broker" + i, getCpuLoad(brokerLoad));
            int bundleLoad = rand.nextInt(brokerLoad + 1);
            topBundlesLoadDataStore.pushAsync("broker" + i, getTopBundlesLoad("bundle" + i,
                    bundleLoad, brokerLoad - bundleLoad));
        }
        return ctx;
    }

    public BrokerLoadData getCpuLoad(int load) {
        var loadData = new BrokerLoadData();
        loadData.setCpu(new ResourceUsage(load, 100));
        loadData.setMsgThroughputIn(load);
        return loadData;
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1, int load2) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.msgThroughputOut = load2;
        var topLoadData = TopBundlesLoadData.of(List.of(
                new TopBundlesLoadData.BundleLoadData(bundlePrefix + "-1", namespaceBundleStats1),
                new TopBundlesLoadData.BundleLoadData(bundlePrefix + "-2", namespaceBundleStats2)),  2);
        return topLoadData;
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var topLoadData = TopBundlesLoadData.of(List.of(
                new TopBundlesLoadData.BundleLoadData(bundlePrefix + "-1", namespaceBundleStats1)), 2);
        return topLoadData;
    }

    public LoadManagerContext getContext(){
        var ctx = mock(LoadManagerContext.class);
        var conf = new ServiceConfiguration();
        var brokerLoadDataStore = new LoadDataStore<BrokerLoadData>() {
            Map<String, BrokerLoadData> map = new HashMap<>();
            @Override
            public void close() throws IOException {

            }

            @Override
            public CompletableFuture<Void> pushAsync(String key, BrokerLoadData loadData) {
                map.put(key, loadData);
                return null;
            }

            @Override
            public Optional<BrokerLoadData> get(String key) {
                var val = map.get(key);
                if (val == null) {
                    return Optional.empty();
                }
                return Optional.of(val);
            }

            @Override
            public void forEach(BiConsumer<String, BrokerLoadData> action) {

            }

            @Override
            public Set<Map.Entry<String, BrokerLoadData>> entrySet() {
                return map.entrySet();
            }
        };

        var topBundleLoadDataStore = new LoadDataStore<TopBundlesLoadData>() {
            Map<String, TopBundlesLoadData> map = new HashMap<>();
            @Override
            public void close() throws IOException {

            }

            @Override
            public CompletableFuture<Void> pushAsync(String key, TopBundlesLoadData loadData) {
                map.put(key, loadData);
                return null;
            }

            @Override
            public Optional<TopBundlesLoadData> get(String key) {
                var val = map.get(key);
                if (val == null) {
                    return Optional.empty();
                }
                return Optional.of(val);
            }

            @Override
            public void forEach(BiConsumer<String, TopBundlesLoadData> action) {

            }

            @Override
            public Set<Map.Entry<String, TopBundlesLoadData>> entrySet() {
                return map.entrySet();
            }
        };
        doReturn(conf).when(ctx).brokerConfiguration();
        doReturn(brokerLoadDataStore).when(ctx).brokerLoadDataStore();
        doReturn(topBundleLoadDataStore).when(ctx).topBundleLoadDataStore();
        return ctx;
    }

    @Test
    public void testEmptyBrokerLoadData() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerExtentionsTransferShedderDebugModeEnabled(true);
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testEmptyTopBundlesLoadData() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerExtentionsTransferShedderDebugModeEnabled(true);
        var brokerLoadDataStore = ctx.brokerLoadDataStore();

        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(1));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(2));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(3));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testOldLoadData() throws IllegalAccessException {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker1").get(), "lastUpdatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker2").get(), "lastUpdatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker3").get(), "lastUpdatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker4").get(), "lastUpdatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker5").get(), "lastUpdatedAt", 0, true);
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testRecentlyUnloadedBrokers() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        Map<String, Long> recentlyUnloadedBrokers = new HashMap<>();
        var oldTS = System.currentTimeMillis() - ctx.brokerConfiguration()
                .getLoadBalancerExtentionsTransferShedderBrokerLoadDataUpdateMinWaitingTimeAfterUnloadingInSeconds() * 1001;
        recentlyUnloadedBrokers.put("broker1", oldTS);
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), recentlyUnloadedBrokers);

        assertEquals(res, List.of(
                new Unload("broker5", "bundleE-1", Optional.of("broker1")),
                new Unload("broker4", "bundleD-2", Optional.of("broker2"))));
        assertEquals(recentlyUnloadedBrokers.size(), 0);

        var now = System.currentTimeMillis();
        recentlyUnloadedBrokers.put("broker1", now);
        res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), recentlyUnloadedBrokers);
        assertEquals(res.size(), 0);
    }

    @Test
    public void testRecentlyUnloadedBundles() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        Map<String, Long> recentlyUnloadedBundles = new HashMap<>();
        var now = System.currentTimeMillis();
        recentlyUnloadedBundles.put("bundleE-1", now);
        recentlyUnloadedBundles.put("bundleE-2", now);
        recentlyUnloadedBundles.put("bundleD-1", now);
        recentlyUnloadedBundles.put("bundleD-2", now);
        var res = transferShedder.findBundlesForUnloading(ctx, recentlyUnloadedBundles, new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testBundlesWithIsolationPolicies() throws IllegalAccessException {
        var pulsar = mock(PulsarService.class);
        TransferShedder transferShedder = new TransferShedder(pulsar);
        var allocationPoliciesSpy = (SimpleResourceAllocationPolicies)
                spy(FieldUtils.readDeclaredField(transferShedder, "allocationPolicies", true));
        doReturn(true).when(allocationPoliciesSpy).areIsolationPoliciesPresent(any());
        FieldUtils.writeDeclaredField(transferShedder, "allocationPolicies", allocationPoliciesSpy, true);
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("my-tenant/my-namespaceA/0x00000000_0xFFFFFFF", 1, 3));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("my-tenant/my-namespaceB/0x00000000_0xFFFFFFF", 2, 8));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("my-tenant/my-namespaceC/0x00000000_0xFFFFFFF", 6, 10));
        topBundlesLoadDataStore.pushAsync("broker4", getTopBundlesLoad("my-tenant/my-namespaceD/0x00000000_0xFFFFFFF", 10, 20));
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("my-tenant/my-namespaceE/0x00000000_0xFFFFFFF", 70, 20));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testBundlesWithAntiAffinityGroup() throws IllegalAccessException, MetadataStoreException {
        var pulsar = mock(PulsarService.class);
        TransferShedder transferShedder = new TransferShedder(pulsar);
        var allocationPoliciesSpy = (SimpleResourceAllocationPolicies)
                spy(FieldUtils.readDeclaredField(transferShedder, "allocationPolicies", true));
        doReturn(false).when(allocationPoliciesSpy).areIsolationPoliciesPresent(any());
        FieldUtils.writeDeclaredField(transferShedder, "allocationPolicies", allocationPoliciesSpy, true);

        var pulsarResourcesMock = mock(PulsarResources.class);
        var localPoliciesResourcesMock = mock(LocalPoliciesResources.class);
        doReturn(pulsarResourcesMock).when(pulsar).getPulsarResources();
        doReturn(localPoliciesResourcesMock).when(pulsarResourcesMock).getLocalPolicies();
        LocalPolicies localPolicies = new LocalPolicies(null, null, "namespaceAntiAffinityGroup");
        doReturn(Optional.of(localPolicies)).when(localPoliciesResourcesMock).getLocalPolicies(any());

        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("my-tenant/my-namespaceA/0x00000000_0xFFFFFFF", 1, 3));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("my-tenant/my-namespaceB/0x00000000_0xFFFFFFF", 2, 8));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("my-tenant/my-namespaceC/0x00000000_0xFFFFFFF", 6, 10));
        topBundlesLoadDataStore.pushAsync("broker4", getTopBundlesLoad("my-tenant/my-namespaceD/0x00000000_0xFFFFFFF", 10, 20));
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("my-tenant/my-namespaceE/0x00000000_0xFFFFFFF", 70, 20));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testTargetStd() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerExtentionsTransferShedderDebugModeEnabled(true);
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(10));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(20));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(30));

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("bundleA", 30, 30));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("bundleB", 40, 40));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("bundleC", 50, 50));

        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }

    @Test
    public void testSingleTopBundlesLoadData() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("bundleA", 1));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("bundleB", 2));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("bundleC", 6));
        topBundlesLoadDataStore.pushAsync("broker4", getTopBundlesLoad("bundleD", 10));
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("bundleE", 70));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res.size(), 0);
    }


    @Test
    public void testTargetStdAfterTransfer() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(55));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(65));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res, List.of(
                new Unload("broker5", "bundleE-1", Optional.of("broker1"))));
    }

    @Test
    public void testMinBrokerWithZeroTraffic() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(55));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(65));

        var load = getCpuLoad(4);
        load.setMsgThroughputIn(0);
        load.setMsgThroughputOut(0);
        brokerLoadDataStore.pushAsync("broker2", load);

        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res, List.of(
                new Unload("broker5", "bundleE-1", Optional.of("broker1")),
                new Unload("broker4", "bundleD-2", Optional.of("broker2"))));
    }

    @Test
    public void testMaxNumberOfTransfersPerShedderCycle() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        ctx.brokerConfiguration()
                .setLoadBalancerExtentionsTransferShedderMaxNumberOfBrokerTransfersPerCycle(10);
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res, List.of(
                new Unload("broker5", "bundleE-1", Optional.of("broker1")),
                new Unload("broker4", "bundleD-2", Optional.of("broker2"))));
    }

    @Test
    public void testRemainingTopBundles() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("bundleE", 20, 20));
        var res = transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
        assertEquals(res, List.of(
                new Unload("broker5", "bundleE-1", Optional.of("broker1")),
                new Unload("broker4", "bundleD-2", Optional.of("broker2"))));
    }

    @Test
    public void testRandomLoad() throws IllegalAccessException {
        TransferShedder transferShedder = new TransferShedder();
        for (int i = 0; i < 5; i++) {
            var ctx = setupContext(10);
            var conf = ctx.brokerConfiguration();
            transferShedder.findBundlesForUnloading(ctx, new HashMap<>(), new HashMap<>());
            var stats = (TransferShedder.LoadStats)
                    FieldUtils.readDeclaredField(transferShedder, "stats", true);
            assertTrue(stats.std <= conf.getLoadBalancerExtentionsTransferShedderTargetLoadStd()
                    || (stats.maxBrokers.isEmpty() || stats.minBrokers.isEmpty() ||
                    stats.minBrokers.peekLast().equals(stats.maxBrokers.peekLast())));
        }
    }

    @Test
    public void testLoadStats() {
        int numBrokers = 10;
        double delta = 0.0001;
        Map<String, Double> brokerAvgResourceUsage = new HashMap<>();
        for (int t = 0; t < 5; t++) {
            var ctx = setupContext(numBrokers);
            TransferShedder.LoadStats stats = new TransferShedder.LoadStats(brokerAvgResourceUsage);
            var conf = ctx.brokerConfiguration();
            stats.update(
                    ctx.brokerLoadDataStore(), conf.getLoadBalancerHistoryResourcePercentage(), new HashMap<>(),
                    conf, true);
            double[] loads = new double[numBrokers];
            var brokerLoadDataStore = ctx.brokerLoadDataStore();
            for (int i = 0; i < loads.length; i++) {
                loads[i] = brokerAvgResourceUsage.get("broker" + i);
            }
            int i = 0;
            int j = loads.length - 1;
            Arrays.sort(loads);
            for (int k = 0; k < conf.getLoadBalancerExtentionsTransferShedderMaxNumberOfBrokerTransfersPerCycle(); k++) {
                double minLoad = loads[i];
                double maxLoad = loads[j];
                double offload = (maxLoad - minLoad) / 2;
                Mean mean = new Mean();
                StandardDeviation std = new StandardDeviation(false);
                assertEquals(minLoad,
                        brokerAvgResourceUsage.get(stats.minBrokers.pollLast()).doubleValue());
                assertEquals(maxLoad,
                        brokerAvgResourceUsage.get(stats.maxBrokers.pollLast()).doubleValue());
                assertEquals(stats.totalBrokers, numBrokers);
                assertEquals(stats.avg, mean.evaluate(loads), delta);
                assertEquals(stats.std, std.evaluate(loads), delta);
                stats.offload(maxLoad, minLoad, offload);
                loads[i++] = minLoad + offload;
                loads[j--] = maxLoad - offload;
            }
        }
    }
}
