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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.Test;

/**
 * Unit test of {@link BrokerLoadData}.
 * TODO: Add more units test.
 */
@Test(groups = "broker")
public class BrokerLoadDataTest {

    @Test
    public void testUpdateBySystemResourceUsage() {

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerCPUResourceWeight(0.5);
        conf.setLoadBalancerMemoryResourceWeight(0.5);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.5);
        conf.setLoadBalancerBandwithInResourceWeight(0.5);
        conf.setLoadBalancerBandwithOutResourceWeight(0.5);
        conf.setLoadBalancerHistoryResourcePercentage(0.75);

        BrokerLoadData data = new BrokerLoadData();

        long now = System.currentTimeMillis();
        SystemResourceUsage usage1 = new SystemResourceUsage();
        var cpu = new ResourceUsage(1.0, 100.0);
        var memory = new ResourceUsage(800.0, 200.0);
        var directMemory= new ResourceUsage(2.0, 100.0);
        var bandwidthIn= new ResourceUsage(3.0, 100.0);
        var bandwidthOut= new ResourceUsage(4.0, 100.0);
        usage1.setCpu(cpu);
        usage1.setMemory(memory);
        usage1.setDirectMemory(directMemory);
        usage1.setBandwidthIn(bandwidthIn);
        usage1.setBandwidthOut(bandwidthOut);
        data.update(usage1, 1, 2, 3, 4, 5, conf);
        
        assertEquals(data.getCpu(), cpu);
        assertEquals(data.getMemory(), memory);
        assertEquals(data.getDirectMemory(), directMemory);
        assertEquals(data.getBandwidthIn(), bandwidthIn);
        assertEquals(data.getBandwidthOut(), bandwidthOut);
        assertEquals(data.getMsgThroughputIn(), 1.0);
        assertEquals(data.getMsgThroughputOut(), 2.0);
        assertEquals(data.getMsgRateIn(), 3.0);
        assertEquals(data.getMsgRateOut(), 4.0);
        assertEquals(data.getBundleCount(), 5);
        assertEquals(data.getMaxResourceUsage(), 0.04); // skips memory usage
        assertEquals(data.getWeightedMaxEMA(), 2);
        assertThat(data.getUpdatedAt(), greaterThanOrEqualTo(now));

        now = System.currentTimeMillis();
        SystemResourceUsage usage2 = new SystemResourceUsage();
        cpu = new ResourceUsage(300.0, 100.0);
        memory = new ResourceUsage(200.0, 200.0);
        directMemory= new ResourceUsage(2.0, 100.0);
        bandwidthIn= new ResourceUsage(3.0, 100.0);
        bandwidthOut= new ResourceUsage(4.0, 100.0);
        usage2.setCpu(cpu);
        usage2.setMemory(memory);
        usage2.setDirectMemory(directMemory);
        usage2.setBandwidthIn(bandwidthIn);
        usage2.setBandwidthOut(bandwidthOut);
        data.update(usage2, 5, 6, 7, 8, 9, conf);

        assertEquals(data.getCpu(), cpu);
        assertEquals(data.getMemory(), memory);
        assertEquals(data.getDirectMemory(), directMemory);
        assertEquals(data.getBandwidthIn(), bandwidthIn);
        assertEquals(data.getBandwidthOut(), bandwidthOut);
        assertEquals(data.getMsgThroughputIn(), 5.0);
        assertEquals(data.getMsgThroughputOut(), 6.0);
        assertEquals(data.getMsgRateIn(), 7.0);
        assertEquals(data.getMsgRateOut(), 8.0);
        assertEquals(data.getBundleCount(), 9);
        assertEquals(data.getMaxResourceUsage(), 3.0);
        assertEquals(data.getWeightedMaxEMA(), 1.875);
        assertThat(data.getUpdatedAt(), greaterThanOrEqualTo(now));
        assertEquals(data.getReportedAt(), 0l);
        assertEquals(data.toString(conf), "cpu= 300.00%, memory= 100.00%, directMemory= 2.00%, "
                + "bandwithIn= 3.00%, bandwithOut= 4.00%, "
                + "cpuWeight= 0.500000, memoryWeight= 0.500000, directMemoryWeight= 0.500000, "
                + "bandwithInResourceWeight= 0.500000, bandwithOutResourceWeight= 0.500000, "
                + "msgThroughputIn= 5.00, msgThroughputOut= 6.00, "
                + "msgRateIn= 7.00, msgRateOut= 8.00, bundleCount= 9, "
                + "maxResourceUsage= 300.00%, weightedMaxEMA= 187.50%, "
                + "updatedAt= " + data.getUpdatedAt() + ", reportedAt= " + data.getReportedAt());
    }

    @Test
    public void testUpdateByBrokerLoadData() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerCPUResourceWeight(0.5);
        conf.setLoadBalancerMemoryResourceWeight(0.5);
        conf.setLoadBalancerDirectMemoryResourceWeight(0.5);
        conf.setLoadBalancerBandwithInResourceWeight(0.5);
        conf.setLoadBalancerBandwithOutResourceWeight(0.5);
        conf.setLoadBalancerHistoryResourcePercentage(0.75);

        BrokerLoadData data = new BrokerLoadData();
        BrokerLoadData other = new BrokerLoadData();

        SystemResourceUsage usage1 = new SystemResourceUsage();
        var cpu = new ResourceUsage(1.0, 100.0);
        var memory = new ResourceUsage(800.0, 200.0);
        var directMemory= new ResourceUsage(2.0, 100.0);
        var bandwidthIn= new ResourceUsage(3.0, 100.0);
        var bandwidthOut= new ResourceUsage(4.0, 100.0);
        usage1.setCpu(cpu);
        usage1.setMemory(memory);
        usage1.setDirectMemory(directMemory);
        usage1.setBandwidthIn(bandwidthIn);
        usage1.setBandwidthOut(bandwidthOut);
        other.update(usage1, 1, 2, 3, 4, 5, conf);
        data.update(other);

        assertEquals(data, other);
    }


}
