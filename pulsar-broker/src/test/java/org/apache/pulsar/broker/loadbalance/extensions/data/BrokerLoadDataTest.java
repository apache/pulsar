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
    }
}
