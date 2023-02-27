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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.Test;


@Test(groups = "broker")
public class TransferShedderTest {
    double setupLoadAvg = 0.36400000000000005;
    double setupLoadStd = 0.3982762860126121;
    public LoadManagerContext setupContext(){
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(ctx,  2));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(ctx,  4));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(ctx,  6));
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(ctx,  80));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(ctx,  90));

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("bundleA", 2000000, 1000000));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("bundleB", 3000000, 1000000));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("bundleC", 4000000, 2000000));
        topBundlesLoadDataStore.pushAsync("broker4", getTopBundlesLoad("bundleD", 6000000, 2000000));
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("bundleE", 7000000, 2000000));
        return ctx;
    }

    public LoadManagerContext setupContext(int clusterSize) {
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        Random rand = new Random();
        for (int i = 0; i < clusterSize; i++) {
            int brokerLoad = rand.nextInt(100);
            brokerLoadDataStore.pushAsync("broker" + i, getCpuLoad(ctx,  brokerLoad));
            int bundleLoad = rand.nextInt(brokerLoad + 1);
            topBundlesLoadDataStore.pushAsync("broker" + i, getTopBundlesLoad("bundle" + i,
                    bundleLoad, brokerLoad - bundleLoad));
        }
        return ctx;
    }

    public BrokerLoadData getCpuLoad(LoadManagerContext ctx, int load) {
        var loadData = new BrokerLoadData();
        SystemResourceUsage usage1 = new SystemResourceUsage();
        var cpu = new ResourceUsage(load, 100.0);
        var memory = new ResourceUsage(0.0, 100.0);
        var directMemory= new ResourceUsage(0.0, 100.0);
        var bandwidthIn= new ResourceUsage(0.0, 100.0);
        var bandwidthOut= new ResourceUsage(0.0, 100.0);
        usage1.setCpu(cpu);
        usage1.setMemory(memory);
        usage1.setDirectMemory(directMemory);
        usage1.setBandwidthIn(bandwidthIn);
        usage1.setBandwidthOut(bandwidthOut);
        loadData.update(usage1, 1,2,3,4,5,
                ctx.brokerConfiguration());
        return loadData;
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1, int load2) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.msgThroughputOut = load2;
        var topKBundles = new TopKBundles();
        topKBundles.update(Map.of(bundlePrefix + "-1", namespaceBundleStats1,
                bundlePrefix + "-2", namespaceBundleStats2), 2);
        return topKBundles.getLoadData();
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var topKBundles = new TopKBundles();
        topKBundles.update(Map.of(bundlePrefix + "-1", namespaceBundleStats1), 2);
        return topKBundles.getLoadData();
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
            public CompletableFuture<Void> removeAsync(String key) {
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

            @Override
            public int size() {
                return map.size();
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
            public CompletableFuture<Void> removeAsync(String key) {
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

            @Override
            public int size() {
                return map.size();
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
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.setReason(NoBrokers);
        assertEquals(res, expected);
    }

    @Test
    public void testEmptyTopBundlesLoadData() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        var brokerLoadDataStore = ctx.brokerLoadDataStore();

        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(ctx,  90));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(ctx,  10));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(ctx,  20));

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.setReason(NoLoadData);
        expected.setLoadAvg(0.39999999999999997);
        expected.setLoadStd(0.35590260840104376);
        assertEquals(res, expected);
    }

    @Test
    public void testOutDatedLoadData() throws IllegalAccessException {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertEquals(res.getUnloads().size(), 2);


        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker1").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker2").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker3").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker4").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker5").get(), "updatedAt", 0, true);


        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.setReason(OutDatedData);
        assertEquals(res, expected);
    }

    @Test
    public void testRecentlyUnloadedBrokers() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();

        Map<String, Long> recentlyUnloadedBrokers = new HashMap<>();
        var oldTS = System.currentTimeMillis() - ctx.brokerConfiguration()
                .getLoadBalancerBrokerLoadDataTTLInSeconds() * 1001;
        recentlyUnloadedBrokers.put("broker1", oldTS);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), recentlyUnloadedBrokers);

        var expected = new UnloadDecision();
        var unloads = expected.getUnloads();
        unloads.put("broker5",
                new Unload("broker5", "bundleE-1", Optional.of("broker1")));
        unloads.put("broker4",
                new Unload("broker4", "bundleD-1", Optional.of("broker2")));

        expected.setLabel(Success);
        expected.setReason(Overloaded);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);

        var now = System.currentTimeMillis();
        recentlyUnloadedBrokers.put("broker1", now);
        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), recentlyUnloadedBrokers);

        expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.setReason(CoolDown);
        assertEquals(res, expected);
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
        var res = transferShedder.findBundlesForUnloading(ctx, recentlyUnloadedBundles, Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.skip(NoBundles);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
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
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.skip(NoBundles);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
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
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.skip(NoBundles);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
    }

    @Test
    public void testTargetStd() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", getCpuLoad(ctx,  10));
        brokerLoadDataStore.pushAsync("broker2", getCpuLoad(ctx,  20));
        brokerLoadDataStore.pushAsync("broker3", getCpuLoad(ctx,  30));

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        topBundlesLoadDataStore.pushAsync("broker1", getTopBundlesLoad("bundleA", 30, 30));
        topBundlesLoadDataStore.pushAsync("broker2", getTopBundlesLoad("bundleB", 40, 40));
        topBundlesLoadDataStore.pushAsync("broker3", getTopBundlesLoad("bundleC", 50, 50));

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.skip(Balanced);
        expected.setLoadAvg(0.2000000063578288);
        expected.setLoadStd(0.08164966587949089);
        assertEquals(res, expected);
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
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        expected.setLabel(Skip);
        expected.skip(NoBundles);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
    }


    @Test
    public void testTargetStdAfterTransfer() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(ctx,  55));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(ctx,  65));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        var unloads = expected.getUnloads();
        unloads.put("broker5",
                new Unload("broker5", "bundleE-1", Optional.of("broker1")));
        expected.setLabel(Success);
        expected.setReason(Overloaded);
        expected.setLoadAvg(0.26400000000000007);
        expected.setLoadStd(0.27644891028904417);
        assertEquals(res, expected);
    }

    @Test
    public void testMinBrokerWithZeroTraffic() throws IllegalAccessException {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4", getCpuLoad(ctx,  55));
        brokerLoadDataStore.pushAsync("broker5", getCpuLoad(ctx,  65));

        var load = getCpuLoad(ctx,  4);
        FieldUtils.writeDeclaredField(load,"msgThroughputIn", 0, true);
        FieldUtils.writeDeclaredField(load,"msgThroughputOut", 0, true);
        brokerLoadDataStore.pushAsync("broker2", load);

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        var unloads = expected.getUnloads();
        unloads.put("broker5",
                new Unload("broker5", "bundleE-1", Optional.of("broker1")));
        unloads.put("broker4",
                new Unload("broker4", "bundleD-1", Optional.of("broker2")));
        expected.setLabel(Success);
        expected.setReason(Underloaded);
        expected.setLoadAvg(0.26400000000000007);
        expected.setLoadStd(0.27644891028904417);
        assertEquals(res, expected);
    }

    @Test
    public void testMaxNumberOfTransfersPerShedderCycle() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        ctx.brokerConfiguration()
                .setLoadBalancerMaxNumberOfBrokerTransfersPerCycle(10);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        var unloads = expected.getUnloads();
        unloads.put("broker5",
                new Unload("broker5", "bundleE-1", Optional.of("broker1")));
        unloads.put("broker4",
                new Unload("broker4", "bundleD-1", Optional.of("broker2")));
        expected.setLabel(Success);
        expected.setReason(Overloaded);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
    }

    @Test
    public void testRemainingTopBundles() {
        TransferShedder transferShedder = new TransferShedder();
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker5", getTopBundlesLoad("bundleE", 3000000, 2000000));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new UnloadDecision();
        var unloads = expected.getUnloads();
        unloads.put("broker5",
                new Unload("broker5", "bundleE-1", Optional.of("broker1")));
        unloads.put("broker4",
                new Unload("broker4", "bundleD-1", Optional.of("broker2")));
        expected.setLabel(Success);
        expected.setReason(Overloaded);
        expected.setLoadAvg(setupLoadAvg);
        expected.setLoadStd(setupLoadStd);
        assertEquals(res, expected);
    }

    @Test
    public void testRandomLoad() throws IllegalAccessException {
        TransferShedder transferShedder = new TransferShedder();
        for (int i = 0; i < 5; i++) {
            var ctx = setupContext(10);
            var conf = ctx.brokerConfiguration();
            transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
            var stats = (TransferShedder.LoadStats)
                    FieldUtils.readDeclaredField(transferShedder, "stats", true);
            assertTrue(stats.std() <= conf.getLoadBalancerBrokerLoadTargetStd()
                    || (!stats.hasTransferableBrokers()));
        }
    }

    @Test
    public void testLoadStats() {
        int numBrokers = 10;
        double delta = 0.0001;
        for (int t = 0; t < 5; t++) {
            var ctx = setupContext(numBrokers);
            TransferShedder.LoadStats stats = new TransferShedder.LoadStats();
            var loadStore = ctx.brokerLoadDataStore();
            stats.setLoadDataStore(loadStore);
            var conf = ctx.brokerConfiguration();
            stats.update(loadStore, Map.of(), conf);
            double[] loads = new double[numBrokers];
            var brokerLoadDataStore = ctx.brokerLoadDataStore();
            for (int i = 0; i < loads.length; i++) {
                loads[i] = loadStore.get("broker" + i).get().getWeightedMaxEMA();
            }
            int i = 0;
            int j = loads.length - 1;
            Arrays.sort(loads);
            for (int k = 0; k < conf.getLoadBalancerMaxNumberOfBrokerTransfersPerCycle(); k++) {
                double minLoad = loads[i];
                double maxLoad = loads[j];
                double offload = (maxLoad - minLoad) / 2;
                Mean mean = new Mean();
                StandardDeviation std = new StandardDeviation(false);
                assertEquals(minLoad,
                        loadStore.get(stats.minBrokers().pollLast()).get().getWeightedMaxEMA());
                assertEquals(maxLoad,
                        loadStore.get(stats.maxBrokers().pollLast()).get().getWeightedMaxEMA());
                assertEquals(stats.totalBrokers(), numBrokers);
                assertEquals(stats.avg(), mean.evaluate(loads), delta);
                assertEquals(stats.std(), std.evaluate(loads), delta);
                stats.offload(maxLoad, minLoad, offload);
                loads[i++] = minLoad + offload;
                loads[j--] = maxLoad - offload;
            }
        }
    }
}
