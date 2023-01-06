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
package org.apache.pulsar.broker.loadbalance.extensions.data;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

/**
 * Unit test of {@link BrokerLoadData}.
 * TODO: Add more units test.
 */
@Test(groups = "broker")
public class BrokerLoadDataTest {

    @Test
    public void testMaxResourceUsage() {
        BrokerLoadData data = new BrokerLoadData();
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
                data.getWeightedMaxEMA(), null);
    }

    @Test
    public void testWeightedMaxEMA() {
        BrokerLoadData data = new BrokerLoadData();
        assertEquals(
                data.getWeightedMaxEMA(), null);
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerCPUResourceWeight(0.5);
        conf.setLoadBalancerMemoryResourceWeight(0.5);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.5);
        conf.setLoadBalancerBandwithInResourceWeight(0.5);
        conf.setLoadBalancerBandwithOutResourceWeight(0.5);
        conf.setLoadBalancerHistoryResourcePercentage(0.75);

        BrokerLoadData data2 = new BrokerLoadData();
        data2.setCpu(new ResourceUsage(1.0, 100.0));
        data2.setMemory(new ResourceUsage(800.0, 200.0));
        data2.setDirectMemory(new ResourceUsage(2.0, 100.0));
        data2.setBandwidthIn(new ResourceUsage(3.0, 100.0));
        data2.setBandwidthOut(new ResourceUsage(4.0, 100.0));
        data.update(data2, conf);
        assertEquals(
                data.getWeightedMaxEMA(), 2);

        BrokerLoadData data3 = new BrokerLoadData();
        data3.setCpu(new ResourceUsage(300.0, 100.0));
        data3.setMemory(new ResourceUsage(200.0, 200.0));
        data3.setDirectMemory(new ResourceUsage(2.0, 100.0));
        data3.setBandwidthIn(new ResourceUsage(3.0, 100.0));
        data3.setBandwidthOut(new ResourceUsage(4.0, 100.0));
        data.update(data3, conf);
        assertEquals(
                data.getWeightedMaxEMA(), 1.875);

    }
}
