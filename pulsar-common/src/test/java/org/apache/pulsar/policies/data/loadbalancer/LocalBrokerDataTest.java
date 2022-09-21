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
package org.apache.pulsar.policies.data.loadbalancer;

import com.google.gson.Gson;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LocalBrokerDataTest {

    @Test
    public void testLocalBrokerDataDeserialization() {
        String data = "{\"webServiceUrl\":\"http://10.244.2.23:8080\",\"webServiceUrlTls\":\"https://10.244.2.23:8081\",\"pulsarServiceUrlTls\":\"pulsar+ssl://10.244.2.23:6651\",\"persistentTopicsEnabled\":true,\"nonPersistentTopicsEnabled\":false,\"cpu\":{\"usage\":3.1577712104798255,\"limit\":100.0},\"memory\":{\"usage\":614.0,\"limit\":1228.0},\"directMemory\":{\"usage\":32.0,\"limit\":1228.0},\"bandwidthIn\":{\"usage\":0.0,\"limit\":0.0},\"bandwidthOut\":{\"usage\":0.0,\"limit\":0.0},\"msgThroughputIn\":0.0,\"msgThroughputOut\":0.0,\"msgRateIn\":0.0,\"msgRateOut\":0.0,\"lastUpdate\":1650886425227,\"lastStats\":{\"pulsar/pulsar/10.244.2.23:8080/0x00000000_0xffffffff\":{\"msgRateIn\":0.0,\"msgThroughputIn\":0.0,\"msgRateOut\":0.0,\"msgThroughputOut\":0.0,\"consumerCount\":0,\"producerCount\":0,\"topics\":1,\"cacheSize\":0}},\"numTopics\":1,\"numBundles\":1,\"numConsumers\":0,\"numProducers\":0,\"bundles\":[\"pulsar/pulsar/10.244.2.23:8080/0x00000000_0xffffffff\"],\"lastBundleGains\":[],\"lastBundleLosses\":[],\"brokerVersionString\":\"2.11.0-hw-0.0.4-SNAPSHOT\",\"protocols\":{},\"advertisedListeners\":{},\"bundleStats\":{\"pulsar/pulsar/10.244.2.23:8080/0x00000000_0xffffffff\":{\"msgRateIn\":0.0,\"msgThroughputIn\":0.0,\"msgRateOut\":0.0,\"msgThroughputOut\":0.0,\"consumerCount\":0,\"producerCount\":0,\"topics\":1,\"cacheSize\":0}},\"maxResourceUsage\":0.49645519256591797,\"loadReportType\":\"LocalBrokerData\"}";
        Gson gson = new Gson();
        LocalBrokerData localBrokerData = gson.fromJson(data, LocalBrokerData.class);
        Assert.assertEquals(localBrokerData.getMemory().limit, 1228.0d, 0.0001f);
        Assert.assertEquals(localBrokerData.getMemory().usage, 614.0d, 0.0001f);
        Assert.assertEquals(localBrokerData.getMemory().percentUsage(), ((float) localBrokerData.getMemory().usage) / ((float) localBrokerData.getMemory().limit) * 100, 0.0001f);
    }

    @Test
    public void testMaxResourceUsage() {
        LocalBrokerData data = new LocalBrokerData();
        data.setCpu(new ResourceUsage(1.0, 100.0));
        data.setMemory(new ResourceUsage(800.0, 200.0));
        data.setDirectMemory(new ResourceUsage(2.0, 100.0));
        data.setBandwidthIn(new ResourceUsage(3.0, 100.0));
        data.setBandwidthOut(new ResourceUsage(4.0, 100.0));

        double epsilon = 0.00001;
        double weight = 0.5;
        // skips memory usage
        assertEquals(data.getMaxResourceUsage(), 0.04, epsilon);

        assertEquals(
                data.getMaxResourceUsageWithWeight(
                        weight, weight, weight, weight, weight), 2.0, epsilon);

        assertEquals(
                data.getMaxResourceUsageWithWeightWithinLimit(
                        weight, weight, weight, weight, weight), 0.02, epsilon);

    }
}
