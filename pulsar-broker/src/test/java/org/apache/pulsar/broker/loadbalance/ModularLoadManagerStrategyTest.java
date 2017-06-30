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

import java.util.Map;

import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.LocalBrokerData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

public class ModularLoadManagerStrategyTest {
    // Test that least long term message rate works correctly.
    @Test
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
        ModularLoadManagerStrategy strategy = new LeastLongTermMessageRate(conf);
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf).equals("1"));
        brokerData1.getTimeAverageData().setLongTermMsgRateIn(400);
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf).equals("2"));
        brokerData2.getLocalData().setCpu(new ResourceUsage(90, 100));
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, conf).equals("3"));
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
