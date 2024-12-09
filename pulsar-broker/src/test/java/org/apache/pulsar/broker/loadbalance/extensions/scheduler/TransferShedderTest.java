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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.AntiAffinityGroupPolicyFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerIsolationPoliciesFilter;
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.policies.AntiAffinityGroupPolicyHelper;
import org.apache.pulsar.broker.loadbalance.extensions.policies.IsolationPoliciesHelper;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "broker")
public class TransferShedderTest {
    double setupLoadAvg = 0.36400000000000005;
    double setupLoadStd = 0.3982762860126121;

    PulsarService pulsar;
    NamespaceService namespaceService;
    ExtensibleLoadManagerWrapper loadManagerWrapper;
    ExtensibleLoadManagerImpl loadManager;
    ServiceUnitStateChannel channel;
    ServiceConfiguration conf;
    IsolationPoliciesHelper isolationPoliciesHelper;
    AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper;
    LocalPoliciesResources localPoliciesResources;
    String bundleD1 = "my-tenant/my-namespaceD/0x00000000_0x0FFFFFFF";
    String bundleD2 = "my-tenant/my-namespaceD/0x0FFFFFFF_0xFFFFFFFF";
    String bundleE1 = "my-tenant/my-namespaceE/0x00000000_0x0FFFFFFF";
    String bundleE2 = "my-tenant/my-namespaceE/0x0FFFFFFF_0xFFFFFFFF";

    @BeforeMethod
    public void init() throws MetadataStoreException {
        pulsar = mock(PulsarService.class);
        loadManagerWrapper = mock(ExtensibleLoadManagerWrapper.class);
        loadManager = mock(ExtensibleLoadManagerImpl.class);
        channel = mock(ServiceUnitStateChannelImpl.class);
        conf = new ServiceConfiguration();
        conf.setLoadBalancerSheddingBundlesWithPoliciesEnabled(true);
        var pulsarResources = mock(PulsarResources.class);
        var namespaceResources = mock(NamespaceResources.class);
        var isolationPolicyResources = mock(NamespaceResources.IsolationPolicyResources.class);
        var factory = mock(NamespaceBundleFactory.class);
        namespaceService = mock(NamespaceService.class);
        localPoliciesResources = mock(LocalPoliciesResources.class);
        isolationPoliciesHelper = mock(IsolationPoliciesHelper.class);
        antiAffinityGroupPolicyHelper = mock(AntiAffinityGroupPolicyHelper.class);
        doReturn(conf).when(pulsar).getConfiguration();
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(pulsarResources).when(pulsar).getPulsarResources();
        doReturn(localPoliciesResources).when(pulsarResources).getLocalPolicies();
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();
        doReturn(isolationPolicyResources).when(namespaceResources).getIsolationPolicies();
        doReturn(Optional.empty()).when(localPoliciesResources).getLocalPolicies(any());
        doReturn(Optional.empty()).when(isolationPolicyResources).getIsolationDataPolicies(any());
        doReturn(factory).when(namespaceService).getNamespaceBundleFactory();
        doAnswer(answer -> {
            String namespace = answer.getArgument(0, String.class);
            String bundleRange = answer.getArgument(1, String.class);
            String[] boundaries = bundleRange.split("_");
            Long lowerEndpoint = Long.decode(boundaries[0]);
            Long upperEndpoint = Long.decode(boundaries[1]);
            Range<Long> hashRange = Range.range(lowerEndpoint, BoundType.CLOSED, upperEndpoint,
                    (upperEndpoint.equals(NamespaceBundles.FULL_UPPER_BOUND)) ? BoundType.CLOSED : BoundType.OPEN);
            return new NamespaceBundle(NamespaceName.get(namespace), hashRange, factory);
        }).when(factory).getBundle(anyString(), anyString());
        doReturn(new AtomicReference<>(loadManagerWrapper)).when(pulsar).getLoadManager();
        doReturn(loadManager).when(loadManagerWrapper).get();
        doReturn(channel).when(loadManager).getServiceUnitStateChannel();
        doReturn(true).when(channel).isOwner(any(), any());
    }

    public LoadManagerContext setupContext(){
        var ctx = getContext();

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 1000000, 2000000));
        topBundlesLoadDataStore.pushAsync("broker2:8080", getTopBundlesLoad("my-tenant/my-namespaceB", 1000000, 3000000));
        topBundlesLoadDataStore.pushAsync("broker3:8080", getTopBundlesLoad("my-tenant/my-namespaceC", 2000000, 4000000));
        topBundlesLoadDataStore.pushAsync("broker4:8080", getTopBundlesLoad("my-tenant/my-namespaceD", 2000000, 6000000));
        topBundlesLoadDataStore.pushAsync("broker5:8080", getTopBundlesLoad("my-tenant/my-namespaceE", 2000000, 7000000));

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 2, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 4, "broker2:8080"));
        brokerLoadDataStore.pushAsync("broker3:8080", getCpuLoad(ctx, 6, "broker3:8080"));
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx, 80, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx, 90, "broker5:8080"));
        return ctx;
    }

    public LoadManagerContext setupContext(int clusterSize) {
        var ctx = getContext();

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        Random rand = new Random();
        for (int i = 0; i < clusterSize; i++) {
            int brokerLoad = rand.nextInt(1000);
            brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));
            int bundleLoad = rand.nextInt(brokerLoad + 1);
            topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                    bundleLoad, brokerLoad - bundleLoad));
        }
        return ctx;
    }

    public LoadManagerContext setupContextLoadSkewedOverload(int clusterSize) {
        var ctx = getContext();

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        int i = 0;
        for (; i < clusterSize-1; i++) {
            int brokerLoad = 1;
            topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                    300_000, 700_000));
            brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));
        }
        int brokerLoad = 100;
        topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                30_000_000, 70_000_000));
        brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));

        return ctx;
    }

    public LoadManagerContext setupContextLoadSkewedUnderload(int clusterSize) {
        var ctx = getContext();

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        int i = 0;
        for (; i < clusterSize-2; i++) {
            int brokerLoad = 98;
            topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                    30_000_000, 70_000_000));
            brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));
        }

        int brokerLoad = 99;
        topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                30_000_000, 70_000_000));
        brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));
        i++;

        brokerLoad = 1;
        topBundlesLoadDataStore.pushAsync("broker" + i + ":8080", getTopBundlesLoad("my-tenant/my-namespace" + i,
                300_000, 700_000));
        brokerLoadDataStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  brokerLoad, "broker" + i + ":8080"));
        return ctx;
    }

    public BrokerLoadData getCpuLoad(LoadManagerContext ctx, int load, String broker) {
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
        if (ctx.topBundleLoadDataStore()
                .get(broker).isPresent()) {
            var throughputOut = ctx.topBundleLoadDataStore()
                    .get(broker).get()
                    .getTopBundlesLoadData().stream().mapToDouble(v -> v.stats().msgThroughputOut).sum();
            loadData.update(usage1, 1, throughputOut, 3, 4, 5, 6,
                    ctx.brokerConfiguration());
        } else {
            loadData.update(usage1, 1, 2, 3, 4, 5, 6,
                    ctx.brokerConfiguration());
        }
        return loadData;
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1, int load2) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.msgThroughputOut = load2;
        var topKBundles = new TopKBundles(pulsar);
        topKBundles.update(Map.of(bundlePrefix + "/0x00000000_0x0FFFFFFF", namespaceBundleStats1,
                bundlePrefix + "/0x0FFFFFFF_0xFFFFFFFF", namespaceBundleStats2), 2);
        return topKBundles.getLoadData();
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix,
                                                int load1, int load2, int load3, int load4, int load5) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.msgThroughputOut = load2;
        var namespaceBundleStats3 = new NamespaceBundleStats();
        namespaceBundleStats3.msgThroughputOut = load3;
        var namespaceBundleStats4 = new NamespaceBundleStats();
        namespaceBundleStats4.msgThroughputOut = load4;
        var namespaceBundleStats5 = new NamespaceBundleStats();
        namespaceBundleStats5.msgThroughputOut = load5;
        var topKBundles = new TopKBundles(pulsar);
        topKBundles.update(Map.of(
                bundlePrefix + "/0x00000000_0x1FFFFFFF", namespaceBundleStats1,
                bundlePrefix + "/0x1FFFFFFF_0x2FFFFFFF", namespaceBundleStats2,
                bundlePrefix + "/0x2FFFFFFF_0x3FFFFFFF", namespaceBundleStats3,
                bundlePrefix + "/0x3FFFFFFF_0x4FFFFFFF", namespaceBundleStats4,
                bundlePrefix + "/0x4FFFFFFF_0x5FFFFFFF", namespaceBundleStats5
        ), 5);
        return topKBundles.getLoadData();
    }

    public TopBundlesLoadData getTopBundlesLoad(String bundlePrefix, int load1) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1;
        var topKBundles = new TopKBundles(pulsar);
        topKBundles.update(Map.of(bundlePrefix + "/0x00000000_0x0FFFFFFF", namespaceBundleStats1), 2);
        return topKBundles.getLoadData();
    }

    public TopBundlesLoadData getTopBundlesLoadWithOutSuffix(String namespace,
                                                             int load1,
                                                             int load2) {
        var namespaceBundleStats1 = new NamespaceBundleStats();
        namespaceBundleStats1.msgThroughputOut = load1 * 1e6;
        var namespaceBundleStats2 = new NamespaceBundleStats();
        namespaceBundleStats2.msgThroughputOut = load2 * 1e6;
        var topKBundles = new TopKBundles(pulsar);
        topKBundles.update(Map.of(namespace + "/0x00000000_0x7FFFFFF", namespaceBundleStats1,
                namespace + "/0x7FFFFFF_0xFFFFFFF", namespaceBundleStats2), 2);
        return topKBundles.getLoadData();
    }

    public LoadManagerContext getContext(){
        var ctx = mock(LoadManagerContext.class);
        var conf = new ServiceConfiguration();
        conf.setLoadBalancerDebugModeEnabled(true);
        conf.setLoadBalancerSheddingBundlesWithPoliciesEnabled(false);
        conf.setLoadBalancerSheddingConditionHitCountThreshold(0);
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

            @Override
            public void closeTableView() throws IOException {

            }

            @Override
            public void start() throws LoadDataStoreException {

            }

            @Override
            public void init() throws IOException {

            }

            @Override
            public void startTableView() throws LoadDataStoreException {

            }

            @Override
            public void startProducer() throws LoadDataStoreException {

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

            @Override
            public void closeTableView() throws IOException {

            }

            @Override
            public void start() throws LoadDataStoreException {

            }

            @Override
            public void init() throws IOException {

            }

            @Override
            public void startTableView() throws LoadDataStoreException {

            }

            @Override
            public void startProducer() throws LoadDataStoreException {

            }
        };

        BrokerRegistry brokerRegistry = mock(BrokerRegistry.class);
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", getMockBrokerLookupData(),
                "broker2:8080", getMockBrokerLookupData(),
                "broker3:8080", getMockBrokerLookupData(),
                "broker4:8080", getMockBrokerLookupData(),
                "broker5:8080", getMockBrokerLookupData()
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();
        doReturn(conf).when(ctx).brokerConfiguration();
        doReturn(brokerLoadDataStore).when(ctx).brokerLoadDataStore();
        doReturn(topBundleLoadDataStore).when(ctx).topBundleLoadDataStore();
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        return ctx;
    }


    BrokerLookupData getMockBrokerLookupData() {
        BrokerLookupData brokerLookupData = mock(BrokerLookupData.class);
        doReturn(true).when(brokerLookupData).persistentTopicsEnabled();
        doReturn(true).when(brokerLookupData).nonPersistentTopicsEnabled();
        return brokerLookupData;
    }

    @Test
    public void testEmptyBrokerLoadData() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBrokers).get(), 1);
    }

    @Test
    public void testNoOwnerLoadData() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        FieldUtils.writeDeclaredField(transferShedder, "channel", channel, true);
        var ctx = setupContext();
        doReturn(false).when(channel).isOwner(any(), any());
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testEmptyTopBundlesLoadData() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();

        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 2, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 4, "broker2:8080"));
        brokerLoadDataStore.pushAsync("broker3:8080", getCpuLoad(ctx, 6, "broker3:8080"));
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx, 80, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx, 90, "broker5:8080"));


        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoLoadData).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testOutDatedLoadData() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertEquals(res.size(), 2);


        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker1:8080").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker2:8080").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker3:8080").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker4:8080").get(), "updatedAt", 0, true);
        FieldUtils.writeDeclaredField(brokerLoadDataStore.get("broker5:8080").get(), "updatedAt", 0, true);


        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(OutDatedData).get(), 1);
    }

    @Test
    public void testRecentlyUnloadedBrokers() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();

        Map<String, Long> recentlyUnloadedBrokers = new HashMap<>();
        var oldTS = System.currentTimeMillis() - ctx.brokerConfiguration()
                .getLoadBalancerBrokerLoadDataTTLInSeconds() * 1001;
        recentlyUnloadedBrokers.put("broker1:8080", oldTS);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), recentlyUnloadedBrokers);

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);

        var now = System.currentTimeMillis();
        recentlyUnloadedBrokers.put("broker1:8080", now);
        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), recentlyUnloadedBrokers);

        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(CoolDown).get(), 1);
    }

    @Test
    public void testRecentlyUnloadedBundles() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        Map<String, Long> recentlyUnloadedBundles = new HashMap<>();
        var now = System.currentTimeMillis();
        recentlyUnloadedBundles.put(bundleE1, now);
        recentlyUnloadedBundles.put(bundleE2, now);
        recentlyUnloadedBundles.put(bundleD1, now);
        recentlyUnloadedBundles.put(bundleD2, now);
        var res = transferShedder.findBundlesForUnloading(ctx, recentlyUnloadedBundles, Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker3:8080",
                "my-tenant/my-namespaceC/0x00000000_0x0FFFFFFF",
                Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testGetAvailableBrokersFailed() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(pulsar, counter, null,
                isolationPoliciesHelper, antiAffinityGroupPolicyHelper);
        var ctx = setupContext();
        BrokerRegistry registry = ctx.brokerRegistry();
        doReturn(FutureUtil.failedFuture(new TimeoutException())).when(registry).getAvailableBrokerLookupDataAsync();
        transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
        assertEquals(counter.getLoadAvg(), 0.0);
        assertEquals(counter.getLoadStd(), 0.0);
    }

    @Test(timeOut = 30 * 1000)
    public void testBundlesWithIsolationPolicies() {
        List<BrokerFilter> filters = new ArrayList<>();
        var allocationPoliciesSpy = mock(SimpleResourceAllocationPolicies.class);
        IsolationPoliciesHelper isolationPoliciesHelper = new IsolationPoliciesHelper(allocationPoliciesSpy);
        BrokerIsolationPoliciesFilter filter = new BrokerIsolationPoliciesFilter(isolationPoliciesHelper);
        filters.add(filter);
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = spy(new TransferShedder(pulsar, counter, filters,
                isolationPoliciesHelper, antiAffinityGroupPolicyHelper));

        setIsolationPolicies(allocationPoliciesSpy, "my-tenant/my-namespaceE",
                Set.of("broker5:8080"), Set.of(), Set.of(), 1);
        var ctx = setupContext();
        ctx.brokerConfiguration().setLoadBalancerSheddingBundlesWithPoliciesEnabled(true);
        doReturn(ctx.brokerConfiguration()).when(pulsar).getConfiguration();
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);

        // Test unload a has isolation policies broker.
        ctx.brokerConfiguration().setLoadBalancerTransferEnabled(false);
        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        expected = new HashSet<>();
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.empty()),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);


        // test setLoadBalancerSheddingBundlesWithPoliciesEnabled=false;
        doReturn(CompletableFuture.completedFuture(true))
                .when(allocationPoliciesSpy).areIsolationPoliciesPresentAsync(any());
        ctx.brokerConfiguration().setLoadBalancerTransferEnabled(true);
        ctx.brokerConfiguration().setLoadBalancerSheddingBundlesWithPoliciesEnabled(false);
        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);

        // Test unload a has isolation policies broker.
        ctx.brokerConfiguration().setLoadBalancerTransferEnabled(false);
        res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 2);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    public BrokerLookupData getLookupData() {
        String webServiceUrl = "http://localhost:8080";
        String webServiceUrlTls = "https://localhoss:8081";
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarServiceUrlTls = "pulsar+ssl://localhost:6651";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        return new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols,
                true, true,
                conf.getLoadManagerClassName(), System.currentTimeMillis(), "3.0.0", Collections.emptyMap());
    }

    private void setIsolationPolicies(SimpleResourceAllocationPolicies policies,
                                      String namespace,
                                      Set<String> primary,
                                      Set<String> secondary,
                                      Set<String> shared,
                                      int min_limit) {
        reset(policies);
        NamespaceName namespaceName = NamespaceName.get(namespace);
        NamespaceBundle namespaceBundle = mock(NamespaceBundle.class);
        doReturn(true).when(namespaceBundle).hasNonPersistentTopic();
        doReturn(namespaceName).when(namespaceBundle).getNamespaceObject();
        doReturn(CompletableFuture.completedFuture(false))
                .when(policies).areIsolationPoliciesPresentAsync(any());
        doReturn(false).when(policies).isPrimaryBroker(any(), any());
        doReturn(false).when(policies).isSecondaryBroker(any(), any());
        doReturn(true).when(policies).isSharedBroker(any());

        doReturn(CompletableFuture.completedFuture(true))
                .when(policies).areIsolationPoliciesPresentAsync(eq(namespaceName));

        primary.forEach(broker -> {
            doReturn(true).when(policies).isPrimaryBroker(eq(namespaceName), eq(broker));
        });

        secondary.forEach(broker -> {
            doReturn(true).when(policies).isSecondaryBroker(eq(namespaceName), eq(broker));
        });

        shared.forEach(broker -> {
            doReturn(true).when(policies).isSharedBroker(eq(broker));
        });

        doAnswer(invocationOnMock -> {
            Integer totalPrimaryCandidates = invocationOnMock.getArgument(1, Integer.class);
            return totalPrimaryCandidates < min_limit;
        }).when(policies).shouldFailoverToSecondaries(eq(namespaceName), anyInt());
    }


    @Test
    public void testBundlesWithAntiAffinityGroup() throws MetadataStoreException {
        var filters = new ArrayList<BrokerFilter>();
        AntiAffinityGroupPolicyFilter filter = new AntiAffinityGroupPolicyFilter(antiAffinityGroupPolicyHelper);
        filters.add(filter);
        var counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(pulsar, counter, filters,
                isolationPoliciesHelper, antiAffinityGroupPolicyHelper);

        LocalPolicies localPolicies = new LocalPolicies(null, null, "namespaceAntiAffinityGroup");
        doReturn(Optional.of(localPolicies)).when(localPoliciesResources).getLocalPolicies(any());

        var ctx = setupContext();
        ctx.brokerConfiguration().setLoadBalancerSheddingBundlesWithPoliciesEnabled(true);

        doAnswer(invocationOnMock -> {
            Map<String, BrokerLookupData> brokers = invocationOnMock.getArgument(0);
            brokers.clear();
            return CompletableFuture.completedFuture(brokers);
        }).when(antiAffinityGroupPolicyHelper).filterAsync(any(), any());
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);

        doAnswer(invocationOnMock -> {
            Map<String, BrokerLookupData> brokers = invocationOnMock.getArgument(0);
            String bundle = invocationOnMock.getArgument(1, String.class);

            if (bundle.equalsIgnoreCase(bundleE1)) {
                return CompletableFuture.completedFuture(brokers);
            }
            brokers.clear();
            return CompletableFuture.completedFuture(brokers);
        }).when(antiAffinityGroupPolicyHelper).filterAsync(any(), any());
        var res2 = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected2 = new HashSet<>();
        expected2.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(res2, expected2);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testFilterHasException() throws MetadataStoreException {
        var filters = new ArrayList<BrokerFilter>();
        BrokerFilter filter = new BrokerFilter() {
            @Override
            public String name() {
                return "Test-Filter";
            }

            @Override
            public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                                ServiceUnitId serviceUnit,
                                                                                LoadManagerContext context) {
                return FutureUtil.failedFuture(new BrokerFilterException("test"));
            }
        };
        filters.add(filter);
        var counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(pulsar, counter, filters,
                isolationPoliciesHelper, antiAffinityGroupPolicyHelper);

        var ctx = setupContext();
        ctx.brokerConfiguration().setLoadBalancerSheddingBundlesWithPoliciesEnabled(true);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testIsLoadBalancerSheddingBundlesWithPoliciesEnabled() {
        var counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(pulsar, counter, new ArrayList<>(),
                isolationPoliciesHelper, antiAffinityGroupPolicyHelper);

        var ctx = setupContext();

        NamespaceBundle namespaceBundle = mock(NamespaceBundle.class);
        doReturn("bundle").when(namespaceBundle).toString();

        boolean[][] expects = {
                {true, true, true, true},
                {true, true, false, false},
                {true, false, true, true},
                {true, false, false, false},
                {false, true, true, true},
                {false, true, false, false},
                {false, false, true, true},
                {false, false, false, true}
        };

        for (boolean[] expect : expects) {
            doReturn(expect[0]).when(isolationPoliciesHelper).hasIsolationPolicy(any());
            doReturn(expect[1]).when(antiAffinityGroupPolicyHelper).hasAntiAffinityGroupPolicy(any());
            ctx.brokerConfiguration().setLoadBalancerSheddingBundlesWithPoliciesEnabled(expect[2]);
            assertEquals(transferShedder.isLoadBalancerSheddingBundlesWithPoliciesEnabled(ctx, namespaceBundle),
                    expect[3]);
        }
    }

    @Test
    public void testTargetStd() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();
        BrokerRegistry brokerRegistry = mock(BrokerRegistry.class);
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", mock(BrokerLookupData.class),
                "broker2:8080", mock(BrokerLookupData.class),
                "broker3:8080", mock(BrokerLookupData.class)
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        ctx.brokerConfiguration().setLoadBalancerDebugModeEnabled(true);
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 10, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 20, "broker2:8080"));
        brokerLoadDataStore.pushAsync("broker3:8080", getCpuLoad(ctx, 30, "broker3:8080"));

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();

        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 30, 30));
        topBundlesLoadDataStore.pushAsync("broker2:8080", getTopBundlesLoad("my-tenant/my-namespaceB", 40, 40));
        topBundlesLoadDataStore.pushAsync("broker3:8080", getTopBundlesLoad("my-tenant/my-namespaceC", 50, 50));

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(HitCount).get(), 1);
        assertEquals(counter.getLoadAvg(), 0.2000000063578288);
        assertEquals(counter.getLoadStd(), 0.08164966587949089);
    }

    @Test
    public void testSingleTopBundlesLoadData() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 1));
        topBundlesLoadDataStore.pushAsync("broker2:8080", getTopBundlesLoad("my-tenant/my-namespaceB", 2));
        topBundlesLoadDataStore.pushAsync("broker3:8080", getTopBundlesLoad("my-tenant/my-namespaceC", 6));
        topBundlesLoadDataStore.pushAsync("broker4:8080", getTopBundlesLoad("my-tenant/my-namespaceD", 10));
        topBundlesLoadDataStore.pushAsync("broker5:8080", getTopBundlesLoad("my-tenant/my-namespaceE", 70));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testBundleThroughputLargerThanOffloadThreshold() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker4:8080", getTopBundlesLoad("my-tenant/my-namespaceD", 1000000000, 1000000000));
        topBundlesLoadDataStore.pushAsync("broker5:8080", getTopBundlesLoad("my-tenant/my-namespaceE", 1000000000, 1000000000));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker3:8080",
                "my-tenant/my-namespaceC/0x00000000_0x0FFFFFFF",
                Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testZeroBundleThroughput() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        for (var e : topBundlesLoadDataStore.entrySet()) {
            for (var stat : e.getValue().getTopBundlesLoadData()) {
                stat.stats().msgThroughputOut = 0;
                stat.stats().msgThroughputIn = 0;

            }
        }
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }


    @Test
    public void testTargetStdAfterTransfer() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx,  55, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx,  65, "broker5:8080"));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), 0.26400000000000007);
        assertEquals(counter.getLoadStd(), 0.27644891028904417);
    }

    @Test
    public void testUnloadBundlesGreaterThanTargetThroughput() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();

        var brokerRegistry = mock(BrokerRegistry.class);
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", mock(BrokerLookupData.class),
                "broker2:8080", mock(BrokerLookupData.class)
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 1000000, 2000000, 3000000, 4000000, 5000000));
        topBundlesLoadDataStore.pushAsync("broker2:8080",
                getTopBundlesLoad("my-tenant/my-namespaceB", 100000000, 180000000, 220000000, 250000000, 250000000));


        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 10, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 1000, "broker2:8080"));


        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(
                new Unload("broker2:8080", "my-tenant/my-namespaceB/0x00000000_0x1FFFFFFF", Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker2:8080", "my-tenant/my-namespaceB/0x1FFFFFFF_0x2FFFFFFF", Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker2:8080", "my-tenant/my-namespaceB/0x2FFFFFFF_0x3FFFFFFF", Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker1:8080", "my-tenant/my-namespaceA/0x00000000_0x1FFFFFFF", Optional.of("broker2:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker1:8080", "my-tenant/my-namespaceA/0x1FFFFFFF_0x2FFFFFFF", Optional.of("broker2:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker1:8080","my-tenant/my-namespaceA/0x2FFFFFFF_0x3FFFFFFF", Optional.of("broker2:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker1:8080","my-tenant/my-namespaceA/0x3FFFFFFF_0x4FFFFFFF", Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(counter.getLoadAvg(), 5.05);
        assertEquals(counter.getLoadStd(), 4.95);
        assertEquals(res, expected);
        var stats = (TransferShedder.LoadStats)
                FieldUtils.readDeclaredField(transferShedder, "stats", true);
        assertEquals(stats.std(), 0.050000004900021836);
    }

    @Test
    public void testSkipBundlesGreaterThanTargetThroughputAfterSplit() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();
        ctx.brokerConfiguration().setLoadBalancerBrokerLoadTargetStd(0.20);

        var brokerRegistry = mock(BrokerRegistry.class);
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", mock(BrokerLookupData.class),
                "broker2:8080", mock(BrokerLookupData.class)
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080",
                getTopBundlesLoad("my-tenant/my-namespaceA", 1, 500000000));
        topBundlesLoadDataStore.pushAsync("broker2:8080",
                getTopBundlesLoad("my-tenant/my-namespaceB", 500000000, 500000000));


        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 50, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 100, "broker2:8080"));


        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        assertTrue(res.isEmpty());
        assertEquals(counter.getBreakdownCounters().get(Skip).get(NoBundles).get(), 1);
    }


    @Test
    public void testUnloadBundlesLessThanTargetThroughputAfterSplit() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();

        var brokerRegistry = mock(BrokerRegistry.class);
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", mock(BrokerLookupData.class),
                "broker2:8080", mock(BrokerLookupData.class)
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 1000000, 2000000, 3000000, 4000000, 5000000));
        topBundlesLoadDataStore.pushAsync("broker2:8080", getTopBundlesLoad("my-tenant/my-namespaceB", 490000000, 510000000));


        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 10, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 1000, "broker2:8080"));


        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(
                new Unload("broker2:8080", "my-tenant/my-namespaceB/0x00000000_0x0FFFFFFF", Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(counter.getLoadAvg(), 5.05);
        assertEquals(counter.getLoadStd(), 4.95);
        assertEquals(res, expected);
        var stats = (TransferShedder.LoadStats)
                FieldUtils.readDeclaredField(transferShedder, "stats", true);
        assertEquals(stats.std(), 0.050000004900021836);

    }


    @Test
    public void testUnloadBundlesGreaterThanTargetThroughputAfterSplit() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = getContext();

        var brokerRegistry = mock(BrokerRegistry.class);
        doReturn(brokerRegistry).when(ctx).brokerRegistry();
        doReturn(CompletableFuture.completedFuture(Map.of(
                "broker1:8080", mock(BrokerLookupData.class),
                "broker2:8080", mock(BrokerLookupData.class)
        ))).when(brokerRegistry).getAvailableBrokerLookupDataAsync();

        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker1:8080", getTopBundlesLoad("my-tenant/my-namespaceA", 2400000, 2400000));
        topBundlesLoadDataStore.pushAsync("broker2:8080", getTopBundlesLoad("my-tenant/my-namespaceB", 5000000, 5000000));


        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1:8080", getCpuLoad(ctx, 48, "broker1:8080"));
        brokerLoadDataStore.pushAsync("broker2:8080", getCpuLoad(ctx, 100, "broker2:8080"));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(
                new Unload("broker1:8080",
                        res.stream().filter(x -> x.getUnload().sourceBroker().equals("broker1:8080")).findFirst().get()
                        .getUnload().serviceUnit(), Optional.of("broker2:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(
                new Unload("broker2:8080",
                        res.stream().filter(x -> x.getUnload().sourceBroker().equals("broker2:8080")).findFirst().get()
                                .getUnload().serviceUnit(), Optional.of("broker1:8080")),
                Success, Overloaded));
        assertEquals(counter.getLoadAvg(), 0.74);
        assertEquals(counter.getLoadStd(), 0.26);
        assertEquals(res, expected);
        var stats = (TransferShedder.LoadStats)
                FieldUtils.readDeclaredField(transferShedder, "stats", true);
        assertEquals(stats.std(), 2.5809568279517847E-8);
    }

    @Test
    public void testMinBrokerWithLowTraffic() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();

        var load = getCpuLoad(ctx, 4, "broker2:8080");
        FieldUtils.writeDeclaredField(load, "msgThroughputEMA", 10, true);


        brokerLoadDataStore.pushAsync("broker2:8080", load);
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx,  55, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx,  65, "broker5:8080"));

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Underloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), 0.26400000000000007);
        assertEquals(counter.getLoadStd(), 0.27644891028904417);
    }

    @Test
    public void testMinBrokerWithLowerLoadThanAvg() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();

        var load = getCpuLoad(ctx,  3 , "broker2:8080");
        brokerLoadDataStore.pushAsync("broker2:8080", load);
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx,  55, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx,  65, "broker5:8080"));

        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Underloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), 0.262);
        assertEquals(counter.getLoadStd(), 0.2780935094532054);
    }

    @Test
    public void testMaxNumberOfTransfersPerShedderCycle() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        ctx.brokerConfiguration()
                .setLoadBalancerMaxNumberOfBrokerSheddingPerCycle(10);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testLoadBalancerSheddingConditionHitCountThreshold() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        int max = 3;
        ctx.brokerConfiguration()
                .setLoadBalancerSheddingConditionHitCountThreshold(max);
        for (int i = 0; i < max; i++) {
            var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
            assertTrue(res.isEmpty());
            assertEquals(counter.getBreakdownCounters().get(Skip).get(HitCount).get(), i+1);
            assertEquals(counter.getLoadAvg(), setupLoadAvg);
            assertEquals(counter.getLoadStd(), setupLoadStd);
        }
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testRemainingTopBundles() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();
        var topBundlesLoadDataStore = ctx.topBundleLoadDataStore();
        topBundlesLoadDataStore.pushAsync("broker5:8080", getTopBundlesLoad("my-tenant/my-namespaceE", 2000000, 3000000));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), setupLoadAvg);
        assertEquals(counter.getLoadStd(), setupLoadStd);
    }

    @Test
    public void testLoadMoreThan100() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContext();

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker4:8080", getCpuLoad(ctx,  200, "broker4:8080"));
        brokerLoadDataStore.pushAsync("broker5:8080", getCpuLoad(ctx,  1000, "broker5:8080"));
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());

        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(new Unload("broker5:8080", bundleE1, Optional.of("broker1:8080")),
                Success, Overloaded));
        expected.add(new UnloadDecision(new Unload("broker4:8080", bundleD1, Optional.of("broker2:8080")),
                Success, Overloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), 2.4240000000000004);
        assertEquals(counter.getLoadStd(), 3.8633332758124816);


        var stats = (TransferShedder.LoadStats)
                FieldUtils.readDeclaredField(transferShedder, "stats", true);
        assertEquals(stats.avg(), 2.4240000000000004);
        assertEquals(stats.std(), 2.781643776903451);
    }

    @Test
    public void testRandomLoad() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
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
    public void testOverloadOutlier() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContextLoadSkewedOverload(100);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        Assertions.assertThat(res).isIn(
                Set.of(new UnloadDecision(
                        new Unload("broker99:8080", "my-tenant/my-namespace99/0x00000000_0x0FFFFFFF",
                                Optional.of("broker52:8080")), Success, Underloaded)),
                Set.of(new UnloadDecision(
                        new Unload("broker99:8080", "my-tenant/my-namespace99/0x00000000_0x0FFFFFFF",
                                Optional.of("broker83:8080")), Success, Underloaded))
        );
        assertEquals(counter.getLoadAvg(), 0.019900000000000008, 0.00001);
        assertEquals(counter.getLoadStd(), 0.09850375627355534, 0.00001);
    }

    @Test
    public void testUnderloadOutlier() {
        UnloadCounter counter = new UnloadCounter();
        TransferShedder transferShedder = new TransferShedder(counter);
        var ctx = setupContextLoadSkewedUnderload(100);
        var res = transferShedder.findBundlesForUnloading(ctx, Map.of(), Map.of());
        var expected = new HashSet<UnloadDecision>();
        expected.add(new UnloadDecision(
                new Unload("broker98:8080", "my-tenant/my-namespace98/0x00000000_0x0FFFFFFF",
                        Optional.of("broker99:8080")), Success, Underloaded));
        assertEquals(res, expected);
        assertEquals(counter.getLoadAvg(), 0.9704000000000005, 0.00001);
        assertEquals(counter.getLoadStd(), 0.09652895938523735, 0.00001);
    }

    @Test
    public void testRandomLoadStats() {
        int numBrokers = 10;
        double delta = 0.0001;
        for (int t = 0; t < 5; t++) {
            var ctx = setupContext(numBrokers);
            TransferShedder.LoadStats stats = new TransferShedder.LoadStats();
            var loadStore = ctx.brokerLoadDataStore();
            stats.setLoadDataStore(loadStore);
            var conf = ctx.brokerConfiguration();
            double[] loads = new double[numBrokers];
            final Map<String, BrokerLookupData> availableBrokers = new HashMap<>();
            for (int i = 0; i < loads.length; i++) {
                availableBrokers.put("broker" + i + ":8080", mock(BrokerLookupData.class));
            }
            stats.update(loadStore, availableBrokers, Map.of(), conf);

            var brokerLoadDataStore = ctx.brokerLoadDataStore();
            for (int i = 0; i < loads.length; i++) {
                loads[i] = loadStore.get("broker" + i + ":8080").get().getWeightedMaxEMA();
            }
            int i = 0;
            int j = loads.length - 1;
            Arrays.sort(loads);
            for (int k = 0; k < conf.getLoadBalancerMaxNumberOfBrokerSheddingPerCycle(); k++) {
                double minLoad = loads[i];
                double maxLoad = loads[j];
                double offload = (maxLoad - minLoad) / 2;
                Mean mean = new Mean();
                StandardDeviation std = new StandardDeviation(false);
                assertEquals(minLoad,
                        loadStore.get(stats.peekMinBroker()).get().getWeightedMaxEMA());
                assertEquals(maxLoad,
                        loadStore.get(stats.pollMaxBroker()).get().getWeightedMaxEMA());
                assertEquals(stats.totalBrokers(), numBrokers);
                assertEquals(stats.avg(), mean.evaluate(loads), delta);
                assertEquals(stats.std(), std.evaluate(loads), delta);
                stats.offload(maxLoad, minLoad, offload);
                loads[i++] = minLoad + offload;
                loads[j--] = maxLoad - offload;
            }
        }
    }

    @Test
    public void testHighVarianceLoadStats() {
        int[] loads = {1, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100};
        var ctx = getContext();
        TransferShedder.LoadStats stats = new TransferShedder.LoadStats();
        var loadStore = ctx.brokerLoadDataStore();
        stats.setLoadDataStore(loadStore);
        var conf = ctx.brokerConfiguration();
        final Map<String, BrokerLookupData> availableBrokers = new HashMap<>();
        for (int i = 0; i < loads.length; i++) {
            availableBrokers.put("broker" + i + ":8080", mock(BrokerLookupData.class));
            loadStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  loads[i], "broker" + i + ":8080"));
        }
        stats.update(loadStore, availableBrokers, Map.of(), conf);

        assertEquals(stats.avg(), 0.9417647058823528);
        assertEquals(stats.std(), 0.23294117647058868);
    }

    @Test
    public void testLowVarianceLoadStats() {
        int[] loads = {390, 391, 392, 393, 394, 395, 396, 397, 398, 399};
        var ctx = getContext();
        TransferShedder.LoadStats stats = new TransferShedder.LoadStats();
        var loadStore = ctx.brokerLoadDataStore();
        stats.setLoadDataStore(loadStore);
        var conf = ctx.brokerConfiguration();
        final Map<String, BrokerLookupData> availableBrokers = new HashMap<>();
        for (int i = 0; i < loads.length; i++) {
            availableBrokers.put("broker" + i + ":8080", mock(BrokerLookupData.class));
            loadStore.pushAsync("broker" + i + ":8080", getCpuLoad(ctx,  loads[i], "broker" + i + ":8080"));
        }
        stats.update(loadStore, availableBrokers, Map.of(), conf);
        assertEquals(stats.avg(), 3.9449999999999994);
        assertEquals(stats.std(), 0.028722813232795824);
    }
}
