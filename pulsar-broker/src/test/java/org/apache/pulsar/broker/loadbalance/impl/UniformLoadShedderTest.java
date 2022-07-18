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

import com.google.common.collect.Multimap;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.assertFalse;

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

}
