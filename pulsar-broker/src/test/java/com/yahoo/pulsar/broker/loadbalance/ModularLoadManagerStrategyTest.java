package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

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
