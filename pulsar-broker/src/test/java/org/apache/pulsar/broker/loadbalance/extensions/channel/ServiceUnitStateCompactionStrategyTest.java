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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
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
        return new ServiceUnitStateData(state, dst, null);
    }
    ServiceUnitStateData data(ServiceUnitState state, String src, String dst) {
        return new ServiceUnitStateData(state, dst, src);
    }

    @Test
    public void test() throws InterruptedException {
        String dst = "dst";
        String src = "src";

        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Init, dst),
                new ServiceUnitStateData(Init, dst, true)));

        assertFalse(strategy.shouldKeepLeft(
                data(Owned), null));

        assertTrue(strategy.shouldKeepLeft(data(Init), data(Init)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Free)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Assigning)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Owned)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Releasing)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Splitting)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data(Deleted)));

        assertTrue(strategy.shouldKeepLeft(data(Assigning), data(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, "dst1"), data(Owned, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, dst), data(Owned, src, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigning, dst), data(Owned, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, src, dst), data(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, src, "dst1"), data(Releasing, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, "src1", dst), data(Releasing, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigning, src, dst), data(Releasing, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data(Splitting, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data(Assigning, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data(Assigning, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data(Assigning, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data(Assigning, dst, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data(Assigning, dst, "dst1")));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data(Releasing, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, "dst1"), data(Releasing, "dst2")));
        assertFalse(strategy.shouldKeepLeft(data(Owned, dst), data(Releasing, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data(Splitting, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, "dst1"), data(Splitting, "dst2")));
        assertFalse(strategy.shouldKeepLeft(data(Owned, dst), data(Splitting, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data(Splitting, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Releasing), data(Init)));
        assertFalse(strategy.shouldKeepLeft(data(Releasing), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "dst1"), data(Free, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "src1", dst), data(Free, "src2", dst)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "dst1"), data(Owned, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, src, "dst1"), data(Owned, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "src1", dst), data(Owned, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Releasing, src, dst), data(Owned, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, src, "dst1"), data(Deleted, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, "dst1"), data(Deleted, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, "src1", dst), data(Deleted, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Splitting, dst), data(Deleted, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Splitting, src, dst), data(Deleted, src, dst)));

        assertFalse(strategy.shouldKeepLeft(data(Deleted), data(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data(Deleted)));

        assertFalse(strategy.shouldKeepLeft(data(Free), data(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Free)));
        assertFalse(strategy.shouldKeepLeft(data(Free), data(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Assigning, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data(Deleted)));
    }
}
