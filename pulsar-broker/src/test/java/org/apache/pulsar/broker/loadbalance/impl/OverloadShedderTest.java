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

import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OverloadShedderTest {

    private final OverloadShedder os = new OverloadShedder();
    private final ServiceConfiguration conf;

    public OverloadShedderTest() {
        conf = new ServiceConfiguration();
        conf.setLoadBalancerBrokerOverloadedThresholdPercentage(85);
    }

    @Test
    public void testNoBrokers() {
        LoadData loadData = new LoadData();
        assertTrue(os.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokersWithNoBundles() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(os.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerNotOverloaded() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(500, 1000));
        broker1.setBandwidthOut(new ResourceUsage(500, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData db1 = new TimeAverageMessageData();
        db1.setMsgThroughputIn(1000);
        db1.setMsgThroughputOut(1000);
        bundle1.setShortTermData(db1);
        loadData.getBundleData().put("bundle-1", bundle1);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(os.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerWithSingleBundle() {
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData db1 = new TimeAverageMessageData();
        db1.setMsgThroughputIn(1000);
        db1.setMsgThroughputOut(1000);
        bundle1.setShortTermData(db1);
        loadData.getBundleData().put("bundle-1", bundle1);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        assertTrue(os.findBundlesForUnloading(loadData, conf).isEmpty());
    }

    @Test
    public void testBrokerWithMultipleBundles() {
        int numBundles = 10;
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));

        LocalBrokerData anotherBroker = new LocalBrokerData();
        String anotherBrokerName = "another-broker";

        double brokerThroghput = 0;

        for (int i = 1; i <= numBundles; i++) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();
            TimeAverageMessageData db = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            db.setMsgThroughputIn(throughput);
            db.setMsgThroughputOut(throughput);
            bundle.setShortTermData(db);
            loadData.getBundleData().put("bundle-" + i, bundle);

            // This bundle should not be selected for `broker1` since it is belong to another broker.
            String anotherBundleName = anotherBrokerName + "-bundle-" + (numBundles + i);
            loadData.getBundleData().put(anotherBundleName, bundle);
            anotherBroker.getBundles().add(anotherBundleName);

            brokerThroghput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroghput);
        broker1.setMsgThroughputOut(brokerThroghput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        loadData.getBrokerData().put(anotherBrokerName, new BrokerData(anotherBroker));

        Multimap<String, String> bundlesToUnload = os.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"), Lists.newArrayList("bundle-10", "bundle-9"));
    }

    @Test
    public void testFilterRecentlyUnloaded() {
        int numBundles = 10;
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));

        double brokerThroghput = 0;

        for (int i = 1; i <= numBundles; i++) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();
            TimeAverageMessageData db = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            db.setMsgThroughputIn(throughput);
            db.setMsgThroughputOut(throughput);
            bundle.setShortTermData(db);
            loadData.getBundleData().put("bundle-" + i, bundle);

            brokerThroghput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroghput);
        broker1.setMsgThroughputOut(brokerThroghput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));

        loadData.getRecentlyUnloadedBundles().put("bundle-10", 1L);
        loadData.getRecentlyUnloadedBundles().put("bundle-9", 1L);

        Multimap<String, String> bundlesToUnload = os.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"), Lists.newArrayList("bundle-8", "bundle-7"));
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
