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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.Multimap;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(groups = "broker")
public class AvgShedderTest {
    private AvgShedder avgShedder;
    private final ServiceConfiguration conf;

    public AvgShedderTest() {
        conf = new ServiceConfiguration();
    }

    @BeforeMethod
    public void setup() {
        avgShedder = new AvgShedder();
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

    @Test
    public void testHitHighThreshold() {
        LoadData loadData = new LoadData();
        BrokerData brokerData1 = initBrokerData();
        BrokerData brokerData2 = initBrokerData();
        BrokerData brokerData3 = initBrokerData();
        loadData.getBrokerData().put("broker1", brokerData1);
        loadData.getBrokerData().put("broker2", brokerData2);
        loadData.getBrokerData().put("broker3", brokerData3);
        // AvgShedder will distribute the load evenly between the highest and lowest brokers
        conf.setMaxUnloadPercentage(0.5);

        // Set the high threshold to 40% and hit count high threshold to 2
        int hitCountForHighThreshold = 2;
        conf.setLoadBalancerAvgShedderHighThreshold(40);
        conf.setLoadBalancerAvgShedderHitCountHighThreshold(hitCountForHighThreshold);
        brokerData1.getLocalData().setCpu(new ResourceUsage(80, 100));
        brokerData2.getLocalData().setCpu(new ResourceUsage(30, 100));
        brokerData1.getLocalData().setMsgRateIn(10000);
        brokerData1.getLocalData().setMsgRateOut(10000);
        brokerData2.getLocalData().setMsgRateIn(1000);
        brokerData2.getLocalData().setMsgRateOut(1000);

        // broker3 is in the middle
        brokerData3.getLocalData().setCpu(new ResourceUsage(50, 100));
        brokerData3.getLocalData().setMsgRateIn(5000);
        brokerData3.getLocalData().setMsgRateOut(5000);

        // expect to shed bundles with message rate(in+out) ((10000+10000)-(1000+1000))/2 = 9000
        // each bundle with 450 msg rate in and 450 msg rate out
        // so 9000/(450+450)=10 bundles will be shed
        for (int i = 0; i < 11; i++) {
            brokerData1.getLocalData().getBundles().add("bundle-" + i);
            BundleData bundle = new BundleData();
            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
            timeAverageMessageData.setMsgRateIn(450);
            timeAverageMessageData.setMsgRateOut(450);
            // as AvgShedder map BundleData to broker, the hashCode of different BundleData should be different
            // so we need to set some different fields to make the hashCode different
            timeAverageMessageData.setNumSamples(i);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle-" + i, bundle);
        }

        // do shedding for the first time, expect to shed nothing because hit count is not enough
        Multimap<String, String> bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 0);

        // do shedding for the second time, expect to shed 10 bundles
        bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 10);

        // assert that all the bundles are shed from broker1
        for (String broker : bundlesToUnload.keys()) {
            assertEquals(broker, "broker1");
        }
        // assert that all the bundles are shed to broker2
        for (String bundle : bundlesToUnload.values()) {
            BundleData bundleData = loadData.getBundleData().get(bundle);
            assertEquals(avgShedder.selectBroker(loadData.getBrokerData().keySet(), bundleData, loadData, conf).get(), "broker2");
        }
    }

    @Test
    public void testHitLowThreshold() {
        LoadData loadData = new LoadData();
        BrokerData brokerData1 = initBrokerData();
        BrokerData brokerData2 = initBrokerData();
        BrokerData brokerData3 = initBrokerData();
        loadData.getBrokerData().put("broker1", brokerData1);
        loadData.getBrokerData().put("broker2", brokerData2);
        loadData.getBrokerData().put("broker3", brokerData3);
        // AvgShedder will distribute the load evenly between the highest and lowest brokers
        conf.setMaxUnloadPercentage(0.5);

        // Set the low threshold to 20% and hit count low threshold to 6
        int hitCountForLowThreshold = 6;
        conf.setLoadBalancerAvgShedderLowThreshold(20);
        conf.setLoadBalancerAvgShedderHitCountLowThreshold(hitCountForLowThreshold);
        brokerData1.getLocalData().setCpu(new ResourceUsage(60, 100));
        brokerData2.getLocalData().setCpu(new ResourceUsage(40, 100));
        brokerData1.getLocalData().setMsgRateIn(10000);
        brokerData1.getLocalData().setMsgRateOut(10000);
        brokerData2.getLocalData().setMsgRateIn(1000);
        brokerData2.getLocalData().setMsgRateOut(1000);

        // broker3 is in the middle
        brokerData3.getLocalData().setCpu(new ResourceUsage(50, 100));
        brokerData3.getLocalData().setMsgRateIn(5000);
        brokerData3.getLocalData().setMsgRateOut(5000);

        // expect to shed bundles with message rate(in+out) ((10000+10000)-(1000+1000))/2 = 9000
        // each bundle with 450 msg rate in and 450 msg rate out
        // so 9000/(450+450)=10 bundles will be shed
        for (int i = 0; i < 11; i++) {
            brokerData1.getLocalData().getBundles().add("bundle-" + i);
            BundleData bundle = new BundleData();
            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
            timeAverageMessageData.setMsgRateIn(450);
            timeAverageMessageData.setMsgRateOut(450);
            // as AvgShedder map BundleData to broker, the hashCode of different BundleData should be different
            // so we need to set some different fields to make the hashCode different
            timeAverageMessageData.setNumSamples(i);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle-" + i, bundle);
        }

        // do shedding for (lowCountForHighThreshold - 1) times, expect to shed nothing because hit count is not enough
        Multimap<String, String> bundlesToUnload;
        for (int i = 0; i < hitCountForLowThreshold - 1; i++) {
            bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
            assertEquals(bundlesToUnload.size(), 0);
        }

        // do shedding for the last time, expect to shed 10 bundles
        bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 10);

        // assert that all the bundles are shed from broker1
        for (String broker : bundlesToUnload.keys()) {
            assertEquals(broker, "broker1");
        }
        // assert that all the bundles are shed to broker2
        for (String bundle : bundlesToUnload.values()) {
            BundleData bundleData = loadData.getBundleData().get(bundle);
            assertEquals(avgShedder.selectBroker(loadData.getBrokerData().keySet(), bundleData, loadData, conf).get(), "broker2");
        }
    }

    @Test
    public void testSheddingMultiplePairs() {
        LoadData loadData = new LoadData();
        BrokerData brokerData1 = initBrokerData();
        BrokerData brokerData2 = initBrokerData();
        BrokerData brokerData3 = initBrokerData();
        BrokerData brokerData4 = initBrokerData();
        loadData.getBrokerData().put("broker1", brokerData1);
        loadData.getBrokerData().put("broker2", brokerData2);
        loadData.getBrokerData().put("broker3", brokerData3);
        loadData.getBrokerData().put("broker4", brokerData4);
        // AvgShedder will distribute the load evenly between the highest and lowest brokers
        conf.setMaxUnloadPercentage(0.5);

        // Set the high threshold to 40% and hit count high threshold to 2
        int hitCountForHighThreshold = 2;
        conf.setLoadBalancerAvgShedderHighThreshold(40);
        conf.setLoadBalancerAvgShedderHitCountHighThreshold(hitCountForHighThreshold);

        // pair broker1 and broker2
        brokerData1.getLocalData().setCpu(new ResourceUsage(80, 100));
        brokerData2.getLocalData().setCpu(new ResourceUsage(30, 100));
        brokerData1.getLocalData().setMsgRateIn(10000);
        brokerData1.getLocalData().setMsgRateOut(10000);
        brokerData2.getLocalData().setMsgRateIn(1000);
        brokerData2.getLocalData().setMsgRateOut(1000);

        // pair broker3 and broker4
        brokerData3.getLocalData().setCpu(new ResourceUsage(75, 100));
        brokerData3.getLocalData().setMsgRateIn(10000);
        brokerData3.getLocalData().setMsgRateOut(10000);
        brokerData4.getLocalData().setCpu(new ResourceUsage(35, 100));
        brokerData4.getLocalData().setMsgRateIn(1000);
        brokerData4.getLocalData().setMsgRateOut(1000);

        // expect to shed bundles with message rate(in+out) ((10000+10000)-(1000+1000))/2 = 9000
        // each bundle with 450 msg rate in and 450 msg rate out
        // so 9000/(450+450)=10 bundles will be shed
        for (int i = 0; i < 11; i++) {
            brokerData1.getLocalData().getBundles().add("bundle1-" + i);
            brokerData3.getLocalData().getBundles().add("bundle3-" + i);

            BundleData bundle = new BundleData();
            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
            timeAverageMessageData.setMsgRateIn(450);
            timeAverageMessageData.setMsgRateOut(450);
            // as AvgShedder map BundleData to broker, the hashCode of different BundleData should be different
            // so we need to set some different fields to make the hashCode different
            timeAverageMessageData.setNumSamples(i);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle1-" + i, bundle);

            bundle = new BundleData();
            timeAverageMessageData = new TimeAverageMessageData();
            timeAverageMessageData.setMsgRateIn(450);
            timeAverageMessageData.setMsgRateOut(450);
            timeAverageMessageData.setNumSamples(i+11);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle3-" + i, bundle);
        }

        // do shedding for the first time, expect to shed nothing because hit count is not enough
        Multimap<String, String> bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 0);

        // do shedding for the second time, expect to shed 10*2=20 bundles
        bundlesToUnload = avgShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 20);

        // assert that half of the bundles are shed from broker1, and the other half are shed from broker3
        for (String broker : bundlesToUnload.keys()) {
            if (broker.equals("broker1")) {
                assertEquals(bundlesToUnload.get(broker).size(), 10);
            } else if (broker.equals("broker3")) {
                assertEquals(bundlesToUnload.get(broker).size(), 10);
            } else {
                fail();
            }
        }

        // assert that all the bundles from broker1 are shed to broker2, and all the bundles from broker3 are shed to broker4
        for (String bundle : bundlesToUnload.values()) {
            BundleData bundleData = loadData.getBundleData().get(bundle);
            if (bundle.startsWith("bundle1-")) {
                assertEquals(avgShedder.selectBroker(loadData.getBrokerData().keySet(), bundleData, loadData, conf).get(), "broker2");
            } else if (bundle.startsWith("bundle3-")) {
                assertEquals(avgShedder.selectBroker(loadData.getBrokerData().keySet(), bundleData, loadData, conf).get(), "broker4");
            } else {
                fail();
            }
        }
    }
}
