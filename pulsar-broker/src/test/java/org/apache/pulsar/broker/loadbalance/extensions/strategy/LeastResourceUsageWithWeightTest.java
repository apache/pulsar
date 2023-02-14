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
package org.apache.pulsar.broker.loadbalance.extensions.strategy;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LeastResourceUsageWithWeightTest {

    // Test that least resource usage with weight works correctly.
    ServiceUnitId bundleData = new ServiceUnitId() {
        @Override
        public NamespaceName getNamespaceObject() {
            return null;
        }

        @Override
        public boolean includes(TopicName topicName) {
            return false;
        }
    };

    public LoadManagerContext setupContext() {
        var ctx = getContext();

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("1", createBrokerData(ctx, 10, 100));
        brokerLoadDataStore.pushAsync("2", createBrokerData(ctx, 30, 100));
        brokerLoadDataStore.pushAsync("3", createBrokerData(ctx, 60, 100));
        brokerLoadDataStore.pushAsync("4", createBrokerData(ctx, 5, 100));

        return ctx;
    }

    public void testSelect() {

        var ctx = setupContext();
        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();

        // Should choice broker from broker1 2 3.
        Set<String> candidates = new HashSet<>();
        candidates.add("1");
        candidates.add("2");
        candidates.add("3");

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("1"));

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("1", createBrokerData(ctx, 20, 100));
        brokerLoadDataStore.pushAsync("2", createBrokerData(ctx, 30, 100));
        brokerLoadDataStore.pushAsync("3", createBrokerData(ctx, 50, 100));

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("1"));

        updateLoad(ctx, "1", 30);
        updateLoad(ctx, "2", 30);
        updateLoad(ctx, "3", 40);

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("1"));

        updateLoad(ctx, "1", 30);
        updateLoad(ctx, "2", 30);
        updateLoad(ctx, "3", 40);

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("1"));

        updateLoad(ctx, "1", 35);
        updateLoad(ctx, "2", 20);
        updateLoad(ctx, "3", 45);

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("2"));

        // test restart broker can load bundle as one of the best brokers.
        updateLoad(ctx, "1", 35);
        updateLoad(ctx, "2", 20);
        brokerLoadDataStore.pushAsync("3", createBrokerData(ctx, 0, 100));

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("3"));
    }

    public void testArithmeticException()
            throws NoSuchFieldException, IllegalAccessException {
        var ctx = setupContext();
        var brokerLoadStore = ctx.brokerLoadDataStore();
        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();

        // Should choice broker from broker1 2 3.
        Set<String> candidates = new HashSet<>();
        candidates.add("1");
        candidates.add("2");
        candidates.add("3");

        FieldUtils.writeDeclaredField(brokerLoadStore.get("1").get(), "weightedMaxEMA", 0.1d, true);
        FieldUtils.writeDeclaredField(brokerLoadStore.get("2").get(), "weightedMaxEMA", 0.3d, true);
        FieldUtils.writeDeclaredField(brokerLoadStore.get("4").get(), "weightedMaxEMA", 0.05d, true);
        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("1"));
    }

    public void testNoLoadDataBrokers() {
        var ctx = setupContext();

        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();

        Set<String> candidates = new HashSet<>();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("1", createBrokerData(ctx,50, 100));
        brokerLoadDataStore.pushAsync("2", createBrokerData(ctx,100, 100));
        brokerLoadDataStore.pushAsync("3", null);
        brokerLoadDataStore.pushAsync("4", null);
        candidates.add("1");
        candidates.add("2");
        candidates.add("5");
        var result = strategy.select(candidates, bundleData, ctx).get();
        assertEquals(result, "1");

        strategy = new LeastResourceUsageWithWeight();
        brokerLoadDataStore.pushAsync("1", createBrokerData(ctx,100, 100));
        result = strategy.select(candidates, bundleData, ctx).get();
        assertThat(result, anyOf(equalTo("1"), equalTo("2"), equalTo("5")));

        brokerLoadDataStore.pushAsync("1", null);
        brokerLoadDataStore.pushAsync("2", null);

        result = strategy.select(candidates, bundleData, ctx).get();
        assertThat(result, anyOf(equalTo("1"), equalTo("2"), equalTo("5")));
    }


    private BrokerLoadData createBrokerData(LoadManagerContext ctx, double usage, double limit) {
        var brokerLoadData = new BrokerLoadData();
        SystemResourceUsage usages = createUsage(usage, limit);
        brokerLoadData.update(usages, 1, 1, 1, 1, 1,
                ctx.brokerConfiguration());
        return brokerLoadData;
    }

    private SystemResourceUsage createUsage(double usage, double limit) {
        SystemResourceUsage usages = new SystemResourceUsage();
        usages.setCpu(new ResourceUsage(usage, limit));
        usages.setMemory(new ResourceUsage(usage, limit));
        usages.setDirectMemory(new ResourceUsage(usage, limit));
        usages.setBandwidthIn(new ResourceUsage(usage, limit));
        usages.setBandwidthOut(new ResourceUsage(usage, limit));
        return usages;
    }

    private void updateLoad(LoadManagerContext ctx, String broker, double usage) {
        ctx.brokerLoadDataStore().get(broker).get().update(createUsage(usage, 100.0),
                1, 1, 1, 1, 1, ctx.brokerConfiguration());
    }

    public static LoadManagerContext getContext() {
        var ctx = mock(LoadManagerContext.class);
        var conf = new ServiceConfiguration();
        conf.setLoadBalancerCPUResourceWeight(1.0);
        conf.setLoadBalancerMemoryResourceWeight(0.1);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.1);
        conf.setLoadBalancerBandwithInResourceWeight(1.0);
        conf.setLoadBalancerBandwithOutResourceWeight(1.0);
        conf.setLoadBalancerHistoryResourcePercentage(0.5);
        conf.setLoadBalancerAverageResourceUsageDifferenceThresholdPercentage(5);
        var brokerLoadDataStore = new LoadDataStore<BrokerLoadData>() {
            Map<String, BrokerLoadData> map = new HashMap<>();
            @Override
            public void close() {

            }

            @Override
            public CompletableFuture<Void> pushAsync(String key, BrokerLoadData loadData) {
                if (loadData == null) {
                    map.remove(key);
                } else {
                    map.put(key, loadData);
                }
                return null;
            }

            @Override
            public CompletableFuture<Void> removeAsync(String key) {
                map.remove(key);
                return CompletableFuture.completedFuture(null);
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

        doReturn(conf).when(ctx).brokerConfiguration();
        doReturn(brokerLoadDataStore).when(ctx).brokerLoadDataStore();
        return ctx;
    }
}
