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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Released;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateCompactionStrategyTest {
    ServiceUnitStateCompactionStrategy strategy = new ServiceUnitStateCompactionStrategy();

    ServiceUnitStateData data(ServiceUnitState state) {
        return new ServiceUnitStateData(state, "broker");
    }

    ServiceUnitStateData data(ServiceUnitState state, String dst) {
        return new ServiceUnitStateData(state, dst, "broker");
    }
    ServiceUnitStateData data(ServiceUnitState state, String src, String dst) {
        return new ServiceUnitStateData(state, dst, src);
    }

    @Test
    public void test() throws InterruptedException {
        String dst = "dst";
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Free)));
        assertFalse(strategy.shouldKeepLeft(data(Free), data(Assigned)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Assigned, "")));
        assertFalse(strategy.shouldKeepLeft(data(Free), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Owned, "")));
        assertFalse(strategy.shouldKeepLeft(data(Free), data(Released)));
        assertFalse(strategy.shouldKeepLeft(data(Free), data(Splitting)));

        assertFalse(strategy.shouldKeepLeft(data(Assigned), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned), data(Assigned)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned, "dst2"), data(Owned, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned, "src1", dst), data(Owned, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigned), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned, "dst2"), data(Released, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned, "src1", dst), data(Released, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigned, dst), data(Released, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigned), data(Splitting)));

        assertFalse(strategy.shouldKeepLeft(data(Owned), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Assigned)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Assigned, "")));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Assigned, "src", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned), data(Assigned, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Released)));
        assertTrue(strategy.shouldKeepLeft(data(Owned,"dst2"), data(Splitting, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned), data(Splitting)));

        assertFalse(strategy.shouldKeepLeft(data(Released), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Released), data(Assigned)));
        assertTrue(strategy.shouldKeepLeft(data(Released, "dst2"), data(Owned, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Released, "src1", dst), data(Owned, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Released), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Released), data(Released)));
        assertTrue(strategy.shouldKeepLeft(data(Released), data(Splitting)));

        assertFalse(strategy.shouldKeepLeft(data(Splitting), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Assigned)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Released)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Splitting)));
    }
}
