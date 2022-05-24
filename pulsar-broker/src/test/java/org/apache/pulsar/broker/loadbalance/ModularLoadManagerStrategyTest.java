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
package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;


import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;
import org.apache.pulsar.broker.loadbalance.impl.ThresholdLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ModularLoadManagerStrategyTest {

    // Test that least long term message rate works correctly.
    public void testLeastLongTermMessageRate() {
        BundleData bundleData = new BundleData();
        BrokerData brokerData1 = initBrokerData();
        BrokerData brokerData2 = initBrokerData();
        BrokerData brokerData3 = initBrokerData();
        brokerData1.getTimeAverageData().setLongTermMsgRateIn(100);
        brokerData2.getTimeAverageData().setLongTermMsgRateIn(200);
        brokerData3.getTimeAverageData().setLongTermMsgRateIn(300);
        LoadData loadData = new LoadData();
        Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        brokerDataMap.put("1", brokerData1);
        brokerDataMap.put("2", brokerData2);
        brokerDataMap.put("3", brokerData3);
        ServiceConfiguration conf = new ServiceConfiguration();
        ModularLoadManagerStrategy strategy = new LeastLongTermMessageRate();
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("1"));
        brokerData1.getTimeAverageData().setLongTermMsgRateIn(400);
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("2"));
        brokerData2.getLocalData().setCpu(new ResourceUsage(90, 100));
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("3"));
    }

    @Test
    public void testThresholdLoadManagerStrategy() {
        BundleData bundleData = new BundleData();
        BrokerData brokerData1 = initBrokerData(40, 100);
        BrokerData brokerData2 = initBrokerData(50, 100);
        BrokerData brokerData3 = initBrokerData(60, 100);

        ServiceConfiguration conf = new ServiceConfiguration();
        ModularLoadManagerStrategy strategy = new ThresholdLoadManagerStrategy();

        LoadData loadData = new LoadData();
        Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        brokerDataMap.put("1", brokerData1);
        brokerDataMap.put("2", brokerData2);
        brokerDataMap.put("3", brokerData3);
        assertTrue(
                Arrays.asList(Optional.of("1"), Optional.of("2"))
                        .contains(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf)));

        brokerDataMap.clear();
        brokerDataMap.put("2", brokerData2);
        brokerDataMap.put("3", brokerData3);
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("2"));

        brokerDataMap.clear();
        brokerDataMap.put("1", brokerData2);
        brokerDataMap.put("3", brokerData3);
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("1"));

        brokerDataMap.clear();
        brokerDataMap.put("3", brokerData3);
        assertEquals(strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf), Optional.of("3"));
    }

    @Test
    public void testCreateModularLoadManagerStrategy() {
        ServiceConfiguration conf = new ServiceConfiguration();

        conf.setLoadBalancerLoadManagerStrategy(LeastLongTermMessageRate.class.getSimpleName());
        assertEquals(ModularLoadManagerStrategy.create(conf).getClass(), LeastLongTermMessageRate.class);

        conf.setLoadBalancerLoadManagerStrategy(ThresholdLoadManagerStrategy.class.getSimpleName());
        assertEquals(ModularLoadManagerStrategy.create(conf).getClass(), ThresholdLoadManagerStrategy.class);

        conf.setLoadBalancerLoadManagerStrategy("ABC");
        assertEquals(ModularLoadManagerStrategy.create(conf).getClass(), LeastLongTermMessageRate.class);
    }

    private BrokerData initBrokerData(double usage, double limit) {
        LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.setCpu(new ResourceUsage(usage, limit));
        localBrokerData.setMemory(new ResourceUsage(usage, limit));
        localBrokerData.setBandwidthIn(new ResourceUsage(usage, limit));
        localBrokerData.setBandwidthOut(new ResourceUsage(usage, limit));
        BrokerData brokerData = new BrokerData(localBrokerData);
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        brokerData.setTimeAverageData(timeAverageBrokerData);
        return brokerData;
    }

    private BrokerData initBrokerData() {
        LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.setCpu(new ResourceUsage());
        localBrokerData.setMemory(new ResourceUsage());
        localBrokerData.setBandwidthIn(new ResourceUsage());
        localBrokerData.setBandwidthOut(new ResourceUsage());
        BrokerData brokerData = new BrokerData(localBrokerData);
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        brokerData.setTimeAverageData(timeAverageBrokerData);
        return brokerData;
    }
}
