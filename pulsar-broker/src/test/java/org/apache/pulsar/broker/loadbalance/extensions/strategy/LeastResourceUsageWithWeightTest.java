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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
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
        brokerLoadDataStore.pushAsync("broker1", initBrokerData(10, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker3", initBrokerData(60, 100));
        brokerLoadDataStore.pushAsync("broker4", initBrokerData(5, 100));

        return ctx;
    }

    public void testSelect() {

        var ctx = setupContext();
        ServiceConfiguration conf = ctx.brokerConfiguration();
        conf.setLoadBalancerCPUResourceWeight(1.0);
        conf.setLoadBalancerMemoryResourceWeight(0.1);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.1);
        conf.setLoadBalancerBandwithInResourceWeight(1.0);
        conf.setLoadBalancerBandwithOutResourceWeight(1.0);
        conf.setLoadBalancerHistoryResourcePercentage(0.5);
        conf.setLoadBalancerAverageResourceUsageDifferenceThresholdPercentage(5);

        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();


        var brokers = ctx.brokerLoadDataStore().entrySet().stream()
                .map(e -> e.getKey()).collect(Collectors.toList());
        // Make brokerAvgResourceUsageWithWeight contain broker4.
        strategy.select(brokers, bundleData, ctx);

        // Should choice broker from broker1 2 3.
        List<String> candidates = new ArrayList<>();
        candidates.add("broker1");
        candidates.add("broker2");
        candidates.add("broker3");

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker1"));

        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", initBrokerData(20, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker3", initBrokerData(50, 100));
        brokerLoadDataStore.pushAsync("broker4", null);

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker1"));

        brokerLoadDataStore.pushAsync("broker1", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker3", initBrokerData(40, 100));

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker1"));

        brokerLoadDataStore.pushAsync("broker1", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(30, 100));
        brokerLoadDataStore.pushAsync("broker3", initBrokerData(40, 100));

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker1"));

        brokerLoadDataStore.pushAsync("broker1", initBrokerData(35, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(20, 100));
        brokerLoadDataStore.pushAsync("broker3", initBrokerData(45, 100));

        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker2"));
    }

    public void testArithmeticException()
            throws NoSuchFieldException, IllegalAccessException {
        var ctx = setupContext();
        var conf = ctx.brokerConfiguration();
        conf.setLoadBalancerCPUResourceWeight(1.0);
        conf.setLoadBalancerMemoryResourceWeight(0.1);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.1);
        conf.setLoadBalancerBandwithInResourceWeight(1.0);
        conf.setLoadBalancerBandwithOutResourceWeight(1.0);
        conf.setLoadBalancerHistoryResourcePercentage(0.5);
        conf.setLoadBalancerAverageResourceUsageDifferenceThresholdPercentage(5);

        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();

        // Should choice broker from broker1 2 3.
        List<String> candidates = new ArrayList<>();
        candidates.add("broker1");
        candidates.add("broker2");
        candidates.add("broker3");
        Field strategyUpdater = LeastResourceUsageWithWeight.class.getDeclaredField("brokerAvgResourceUsageWithWeight");
        strategyUpdater.setAccessible(true);
        Map<String, Double> brokerAvgResourceUsageWithWeight = new HashMap<>();
        brokerAvgResourceUsageWithWeight.put("broker1", 0.1d);
        brokerAvgResourceUsageWithWeight.put("broker2", 0.3d);
        brokerAvgResourceUsageWithWeight.put("broker4", 0.05d);
        strategyUpdater.set(strategy, brokerAvgResourceUsageWithWeight);
        assertEquals(strategy.select(candidates, bundleData, ctx), Optional.of("broker1"));
    }

    public void testNoLoadDataBrokers() {
        var ctx = setupContext();

        LeastResourceUsageWithWeight strategy = new LeastResourceUsageWithWeight();

        List<String> candidates = new ArrayList<>();
        var brokerLoadDataStore = ctx.brokerLoadDataStore();
        brokerLoadDataStore.pushAsync("broker1", initBrokerData(50, 100));
        brokerLoadDataStore.pushAsync("broker2", initBrokerData(100, 100));
        brokerLoadDataStore.pushAsync("broker3", null);
        brokerLoadDataStore.pushAsync("broker4", null);
        candidates.add("broker1");
        candidates.add("broker2");
        candidates.add("broker5");
        var result = strategy.select(candidates, bundleData, ctx).get();
        assertEquals(result, "broker1");

        strategy = new LeastResourceUsageWithWeight();
        brokerLoadDataStore.pushAsync("broker1", initBrokerData(100, 100));
        result = strategy.select(candidates, bundleData, ctx).get();
        assertThat(result, anyOf(equalTo("broker1"), equalTo("broker2"), equalTo("broker5")));

        brokerLoadDataStore.pushAsync("broker1", null);
        brokerLoadDataStore.pushAsync("broker2", null);

        result = strategy.select(candidates, bundleData, ctx).get();
        assertThat(result, anyOf(equalTo("broker1"), equalTo("broker2"), equalTo("broker5")));
    }


    private BrokerLoadData initBrokerData(double usage, double limit) {
        var brokerLoadData = new BrokerLoadData();
        brokerLoadData.setCpu(new ResourceUsage(usage, limit));
        brokerLoadData.setMemory(new ResourceUsage(usage, limit));
        brokerLoadData.setDirectMemory(new ResourceUsage(usage, limit));
        brokerLoadData.setBandwidthIn(new ResourceUsage(usage, limit));
        brokerLoadData.setBandwidthOut(new ResourceUsage(usage, limit));
        return brokerLoadData;
    }

    public LoadManagerContext getContext() {
        var ctx = mock(LoadManagerContext.class);
        var conf = new ServiceConfiguration();
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

        doReturn(conf).when(ctx).brokerConfiguration();
        doReturn(brokerLoadDataStore).when(ctx).brokerLoadDataStore();
        return ctx;
    }
}
