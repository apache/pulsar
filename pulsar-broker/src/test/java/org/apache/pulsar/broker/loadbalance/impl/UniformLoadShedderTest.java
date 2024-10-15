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
import org.apache.pulsar.policies.data.loadbalancer.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
public class UniformLoadShedderTest {
    private UniformLoadShedder uniformLoadShedder;

    private final ServiceConfiguration conf;

    public UniformLoadShedderTest() {
        conf = new ServiceConfiguration();
    }

    @BeforeMethod
    public void setup() {
        uniformLoadShedder = new UniformLoadShedder();
    }

    @Test
    public void testMaxUnloadBundleNumPerShedding(){
        conf.setMaxUnloadBundleNumPerShedding(2);
        int numBundles = 20;
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
        LocalBrokerData broker2 = new LocalBrokerData();

        String broker2Name = "broker2";

        double brokerThroughput = 0;

        for (int i = 1; i <= numBundles; ++i) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();

            TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();

            double throughput = 1 * 1024 * 1024;
            timeAverageMessageData.setMsgThroughputIn(throughput);
            timeAverageMessageData.setMsgThroughputOut(throughput);
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put("bundle-" + i, bundle);

            brokerThroughput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroughput);
        broker1.setMsgThroughputOut(brokerThroughput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        loadData.getBrokerData().put(broker2Name, new BrokerData(broker2));

        Multimap<String, String> bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(),2);
    }

    @Test
    public void testBrokerWithMultipleBundles() {
        int numBundles = 10;
        LoadData loadData = new LoadData();

        LocalBrokerData broker1 = new LocalBrokerData();
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

            brokerThroughput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroughput);
        broker1.setMsgThroughputOut(brokerThroughput);

        loadData.getBrokerData().put("broker-1", new BrokerData(broker1));
        loadData.getBrokerData().put(broker2Name, new BrokerData(broker2));

        Multimap<String, String> bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
    }

    @Test
    public void testOverloadBrokerSelect() {
        conf.setMaxUnloadBundleNumPerShedding(1);
        conf.setMaxUnloadPercentage(0.5);
        int numBrokers = 5;
        int numBundles = 5;
        LoadData loadData = new LoadData();

        LocalBrokerData[] localBrokerDatas = new LocalBrokerData[]{
                new LocalBrokerData(),
                new LocalBrokerData(),
                new LocalBrokerData(),
                new LocalBrokerData(),
                new LocalBrokerData()};

        String[] brokerNames = new String[]{"broker0", "broker1", "broker2", "broker3", "broker4"};

        double[] brokerMsgRates = new double[]{
                50000, // broker0
                60000, // broker1
                70000, // broker2
                10000, // broker3
                20000};// broker4

        double[] brokerMsgThroughputs = new double[]{
                50 * 1024 * 1024, // broker0
                60 * 1024 * 1024, // broker1
                70 * 1024 * 1024, // broker2
                80 * 1024 * 1024, // broker3
                10 * 1024 * 1024};// broker4


        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            // set LocalBrokerData first
            localBrokerDatas[brokerId].setMsgRateIn(brokerMsgRates[brokerId]);
            localBrokerDatas[brokerId].setMsgThroughputIn(brokerMsgThroughputs[brokerId]);
            // then set bundle data
            double msgRate = brokerMsgRates[brokerId] / numBundles;
            double throughput = brokerMsgThroughputs[brokerId] / numBundles;
            for (int i = 0; i < numBundles; ++i) {
                String bundleName = "broker-" + brokerId + "-bundle-" + i;
                localBrokerDatas[brokerId].getBundles().add(bundleName);
                BundleData bundle = new BundleData();

                TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
                timeAverageMessageData.setMsgRateIn(msgRate);
                timeAverageMessageData.setMsgThroughputIn(throughput);
                bundle.setShortTermData(timeAverageMessageData);
                loadData.getBundleData().put(bundleName, bundle);
            }
            loadData.getBrokerData().put(brokerNames[brokerId], new BrokerData(localBrokerDatas[brokerId]));
        }

        // disable throughput based load shedding, enable rate based load shedding only
        conf.setLoadBalancerMsgRateDifferenceShedderThreshold(50);
        conf.setLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold(0);

        Multimap<String, String> bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 1);
        assertTrue(bundlesToUnload.containsKey("broker2"));


        // disable rate based load shedding, enable throughput based load shedding only
        conf.setLoadBalancerMsgRateDifferenceShedderThreshold(0);
        conf.setLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold(2);

        bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 1);
        assertTrue(bundlesToUnload.containsKey("broker3"));

        // enable both rate and throughput based load shedding, but rate based load shedding has higher priority
        conf.setLoadBalancerMsgRateDifferenceShedderThreshold(50);
        conf.setLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold(2);

        bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 1);
        assertTrue(bundlesToUnload.containsKey("broker2"));
    }

    @Test
    public void testFilterOutNonTrafficBundlesForUnloading() {
        conf.setMaxUnloadBundleNumPerShedding(5);
        conf.setMaxUnloadPercentage(0.5);
        LoadData loadData = new LoadData();

        String[] brokerNames = new String[]{"broker0", "broker1"};

        double[] brokerMsgRates = new double[]{
                10_000, // broker0
                100_000};// broker1

        double[] brokerMsgThroughputs = new double[]{
                10 * 1024 * 1024, // broker0  10MB
                100 * 1024 * 1024};// broker1 100MB

        LocalBrokerData brokerData0 = new LocalBrokerData();
        LocalBrokerData brokerData1 = new LocalBrokerData();

        // set broker data
        brokerData0.setMsgRateIn(brokerMsgRates[0]);
        brokerData0.setMsgThroughputIn(brokerMsgThroughputs[0]);
        brokerData1.setMsgRateIn(brokerMsgRates[1]);
        brokerData1.setMsgThroughputIn(brokerMsgThroughputs[1]);

        // broker0 bundle data
        String bundleName = "broker-0-bundle-0";
        brokerData0.getBundles().add(bundleName);
        double bundleMsgRate = brokerMsgRates[0];
        double bundleMsgThroughput = brokerMsgThroughputs[0];
        TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
        timeAverageMessageData.setMsgRateIn(bundleMsgRate);
        timeAverageMessageData.setMsgThroughputIn(bundleMsgThroughput);
        BundleData bundle = new BundleData();
        bundle.setShortTermData(timeAverageMessageData);
        loadData.getBundleData().put(bundleName, bundle);

        // broker1 bundle data, 5 bundles and some of them are non traffic bundles
        // details are:
        // bundleName   msgRate   msgThroughput
        // bundle-0     60_000     60 MB
        // bundle-1     20_000     60 MB
        // bundle-2     20_000     60 MB
        // bundle-3     0           0 MB
        // bundle-4     0           0 MB
        double[] broker1BundleMsgRates = new double[]{
                60_000, 20_000, 20_000, 0, 0};
        double[] broker1BundleMsgThroughputs = new double[]{
                60 * 1024 * 1024, 20 * 1024 * 1024, 20 * 1024 * 1024, 0, 0};
        for (int i = 0 ; i < 5; i++) {
            bundleName = "broker-1-bundle-" + i;
            brokerData1.getBundles().add(bundleName);
            bundleMsgRate = broker1BundleMsgRates[i];
            bundleMsgThroughput = broker1BundleMsgThroughputs[i];
            timeAverageMessageData = new TimeAverageMessageData();
            timeAverageMessageData.setMsgRateIn(bundleMsgRate);
            timeAverageMessageData.setMsgThroughputIn(bundleMsgThroughput);
            bundle = new BundleData();
            bundle.setShortTermData(timeAverageMessageData);
            loadData.getBundleData().put(bundleName, bundle);
        }

        loadData.getBrokerData().put(brokerNames[0], new BrokerData(brokerData0));
        loadData.getBrokerData().put(brokerNames[1], new BrokerData(brokerData1));

        // disable throughput based load shedding, enable rate based load shedding only
        conf.setLoadBalancerMsgRateDifferenceShedderThreshold(50);
        conf.setLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold(0);

        Multimap<String, String> bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 2);
        assertTrue(bundlesToUnload.containsValue("broker-1-bundle-1"));
        assertTrue(bundlesToUnload.containsValue("broker-1-bundle-2"));


        // disable rate based load shedding, enable throughput based load shedding only
        conf.setLoadBalancerMsgRateDifferenceShedderThreshold(0);
        conf.setLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold(2);

        bundlesToUnload = uniformLoadShedder.findBundlesForUnloading(loadData, conf);
        assertEquals(bundlesToUnload.size(), 2);
        assertTrue(bundlesToUnload.containsValue("broker-1-bundle-1"));
        assertTrue(bundlesToUnload.containsValue("broker-1-bundle-2"));
    }

}
