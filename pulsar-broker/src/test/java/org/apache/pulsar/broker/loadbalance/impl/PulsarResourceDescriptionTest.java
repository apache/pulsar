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

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarResourceDescriptionTest {
    @Test
    public void compareTo() {
        PulsarResourceDescription one = new PulsarResourceDescription();
        one.put("cpu", new ResourceUsage(0.1, 0.2));
        PulsarResourceDescription two = new PulsarResourceDescription();
        two.put("cpu", new ResourceUsage(0.1, 0.2));
        assertEquals(0, one.compareTo(two));
    }
}
