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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class ThresholdShedderTest {
    private ThresholdShedder thresholdShedder;
    private final ServiceConfiguration conf;

    public ThresholdShedderTest() {
        conf = new ServiceConfiguration();
    }

    @BeforeMethod
    public void setup() {
        thresholdShedder = new ThresholdShedder();
    }

    @Test
    public void testNoBrokers() {
        LoadData loadData = new LoadData();
        assertTrue(thresholdShedder.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokersWithNoBundles() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(thresholdShedder.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerNotReachThreshold() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(500, 1000));
        broker1.setBandwidthOut(new ResourceUsage(500, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundleData = new BundleData();
        TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
        timeAverageMessageData.setMsgThroughputIn(1000);
        timeAverageMessageData.setMsgThroughputOut(1000);
        bundleData.setShortTermData(timeAverageMessageData);
        loadData.getBundleData().put("bundle-1", bundleData);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        assertTrue(thresholdShedder.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerWithSingleBundle() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
        timeAverageMessageData.setMsgThroughputIn(1000);
        timeAverageMessageData.setMsgThroughputOut(1000);
        bundle1.setShortTermData(timeAverageMessageData);
        loadData.getBundleData().put("bundle-1", bundle1);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(thresholdShedder.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerWithMultipleBundles() {
        int numBundles = 10;
        LoadData loadData = new LoadData();
        
        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));

        LocalBrokerData broker2 = new LocalBrokerData();

        String broker2Name = "broker2";

        double brokerThroughput = 0;

        for (int i = 1; i <= numBundles; ++i) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();

            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            timeAverageMessageData.setMsgThroughputIn(throughput);
            timeAverageMessageData.setMsgThroughputOut(throughput);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle-" + i, bundle);

            // This bundle should not be selected for `broker1` since it is belong to another broker.
            String broker2BundleName = broker2Name + "-bundle-" + (numBundles + i);
            loadData.getBundleData().put(broker2BundleName, bundle);
            broker2.getBundles().add(broker2BundleName);

            brokerThroughput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroughput);
        broker1.setMsgThroughputOut(brokerThroughput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        loadData.getBrokerData().put(broker2Name, new BrokerData(broker2));

        Multimap<String, String> bundlesToUnload = thresholdShedder.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"),
            Lists.newArrayList("bundle-10", "bundle-9", "bundle-8"));
    }

    @Test
    public void testFilterRecentlyUnloaded() {
        int numBundles = 10;
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));

        LocalBrokerData broker2 = new LocalBrokerData();
        String broker2Name = "broker2";

        double brokerThroughput = 0;
        for (int i = 1; i <= numBundles; ++i) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundleData = new BundleData();
            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            timeAverageMessageData.setMsgThroughputIn(throughput);
            timeAverageMessageData.setMsgThroughputOut(throughput);
            bundleData.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle-" + i, bundleData);

            // This bundle should not be selected for `broker1` since it is belong to another broker.
            String broker2BundleName = broker2Name + "-bundle-" + (numBundles + i);
            loadData.getBundleData().put(broker2BundleName, bundleData);
            broker2.getBundles().add(broker2BundleName);

            brokerThroughput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroughput);
        broker1.setMsgThroughputOut(brokerThroughput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        loadData.getBrokerData().put(broker2Name, new BrokerData(broker2));

        loadData.getRecentlyUnloadedBundles().put("bundle-10", 1L);
        loadData.getRecentlyUnloadedBundles().put("bundle-9", 1L);

        Multimap<String, String> bundlesToUnload = thresholdShedder.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"),
            Lists.newArrayList("bundle-8", "bundle-7", "bundle-6", "bundle-5"));
    }

    @Test
    public void testPrintResourceUsage() {
        LocalBrokerData data = new LocalBrokerData();

        data.setCpu(new ResourceUsage(10, 100));
        data.setMemory(new ResourceUsage(50, 100));
        data.setDirectMemory(new ResourceUsage(90, 100));
        data.setBandwidthIn(new ResourceUsage(30, 100));
        data.setBandwidthOut(new ResourceUsage(20, 100));

        assertEquals(data.printResourceUsage(),
            "cpu: 10.00%, memory: 50.00%, directMemory: 90.00%, bandwidthIn: 30.00%, bandwidthOut: 20.00%");
    }
}
