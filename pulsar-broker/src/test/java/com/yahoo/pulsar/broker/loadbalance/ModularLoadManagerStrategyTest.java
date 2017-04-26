/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance;

import java.util.Map;

import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.TimeAverageBundleData;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ResourceUsage;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;

public class ModularLoadManagerStrategyTest {
    // Test that least long term message rate works correctly.
    @Test
    public void testLeastLongTermMessageRate() {
        TimeAverageBundleData bundleData = new TimeAverageBundleData();
        BrokerData brokerData1 = initBrokerData();
        BrokerData brokerData2 = initBrokerData();
        BrokerData brokerData3 = initBrokerData();
        brokerData1.getTimeAverageData().getLongTermData().setMsgRateIn(100);
        brokerData2.getTimeAverageData().getLongTermData().setMsgRateIn(200);
        brokerData3.getTimeAverageData().getLongTermData().setMsgRateIn(300);
        LoadData loadData = new LoadData();
        Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        brokerDataMap.put("1", brokerData1);
        brokerDataMap.put("2", brokerData2);
        brokerDataMap.put("3", brokerData3);
        ServiceConfiguration conf = new ServiceConfiguration();
        PulsarService pulsar = new PulsarService(conf);
        ModularLoadManagerStrategy strategy = new LeastLongTermMessageRate(pulsar);
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, pulsar).equals("1"));
        brokerData1.getTimeAverageData().getLongTermData().setMsgRateIn(400);
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, pulsar).equals("2"));
        brokerData2.getLocalData().getSystemResourceUsage().cpu = new ResourceUsage(90, 100);
        assert (strategy.selectBroker(brokerDataMap.keySet(), bundleData, loadData, pulsar).equals("3"));
    }

    private BrokerData initBrokerData() {
        SystemResourceUsage systemResourceUsage = new SystemResourceUsage();
        LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.setSystemResourceUsage(systemResourceUsage);
        BrokerData brokerData = new BrokerData(localBrokerData);
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        brokerData.setTimeAverageData(timeAverageBrokerData);
        return brokerData;
    }
}
