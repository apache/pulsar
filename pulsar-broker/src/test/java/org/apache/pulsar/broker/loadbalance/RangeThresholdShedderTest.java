/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.RangeThresholdShedder;
import org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder;
import org.apache.pulsar.broker.loadbalance.impl.ThresholdShedderTest;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class RangeThresholdShedderTest extends ThresholdShedderTest {
    private final ServiceConfiguration conf = new ServiceConfiguration();

    @BeforeMethod
    public void setup() {
        super.thresholdShedder = new RangeThresholdShedder();
    }

    @Test
    public void testRangeThroughput() {
        int numBundles = 10;
        int brokerNum = 11;
        int lowLoadNode = 10;
        LoadData loadData = new LoadData();
        double throughput = 50 * ThresholdShedder.MB;
        //There are 11 Brokers, of which 10 are loaded at 80% and 1 is loaded at 0%.
        //At this time, the average load is 80*10/11 = 72.73, and the threshold for rebalancing is 72.73 + 10 = 82.73.
        //Since 80 < 82.73, rebalancing will not be trigger, and there is one Broker with load of 0.
        for (int i = 0; i < brokerNum; i++) {
            LocalBrokerData broker = new LocalBrokerData();
            for (int j = 0; j < numBundles; j++) {
                broker.getBundles().add("bundle-" + j);
                BundleData bundle = new BundleData();
                TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
                timeAverageMessageData.setMsgThroughputIn(i == lowLoadNode ? 0 : throughput);
                timeAverageMessageData.setMsgThroughputOut(i == lowLoadNode ? 0 : throughput);
                bundle.setShortTermData(timeAverageMessageData);
                String broker2BundleName = "broker-" + i + "-bundle-" + (numBundles + i);
                loadData.getBundleData().put(broker2BundleName, bundle);
                broker.getBundles().add(broker2BundleName);
            }
            broker.setBandwidthIn(new ResourceUsage(i == lowLoadNode ? 0 : 80, 100));
            broker.setBandwidthOut(new ResourceUsage(i == lowLoadNode ? 0 : 80, 100));
            broker.setMsgThroughputIn(i == lowLoadNode ? 0 : throughput);
            broker.setMsgThroughputOut(i == lowLoadNode ? 0 : throughput);
            loadData.getBrokerData().put("broker-" + i, new BrokerData(broker));
        }
        ThresholdShedder shedder = new ThresholdShedder();
        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(loadData, conf);
        assertTrue(bundlesToUnload.isEmpty());
        bundlesToUnload = thresholdShedder.findBundlesForUnloading(loadData, conf);
        assertFalse(bundlesToUnload.isEmpty());
    }

    @Test
    public void testNoBrokerToOffload() {
        int numBundles = 10;
        int brokerNum = 11;
        LoadData loadData = new LoadData();
        double throughput = 80 * ThresholdShedder.MB;
        //Load of all Brokers are 80%, and no Broker needs to offload.
        for (int i = 0; i < brokerNum; i++) {
            LocalBrokerData broker = new LocalBrokerData();
            for (int j = 0; j < numBundles; j++) {
                broker.getBundles().add("bundle-" + j);
                BundleData bundle = new BundleData();
                TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
                timeAverageMessageData.setMsgThroughputIn(throughput);
                timeAverageMessageData.setMsgThroughputOut(throughput);
                bundle.setShortTermData(timeAverageMessageData);
                String broker2BundleName = "broker-" + i + "-bundle-" + (numBundles + i);
                loadData.getBundleData().put(broker2BundleName, bundle);
                broker.getBundles().add(broker2BundleName);
            }
            broker.setBandwidthIn(new ResourceUsage(80, 100));
            broker.setBandwidthOut(new ResourceUsage(80, 100));
            broker.setMsgThroughputIn(throughput);
            broker.setMsgThroughputOut(throughput);
            loadData.getBrokerData().put("broker-" + i, new BrokerData(broker));
        }
        ThresholdShedder shedder = new ThresholdShedder();
        Multimap<String, String> bundlesToUnload = shedder.findBundlesForUnloading(loadData, conf);
        assertTrue(bundlesToUnload.isEmpty());
        bundlesToUnload = thresholdShedder.findBundlesForUnloading(loadData, conf);
        assertTrue(bundlesToUnload.isEmpty());
    }

}
