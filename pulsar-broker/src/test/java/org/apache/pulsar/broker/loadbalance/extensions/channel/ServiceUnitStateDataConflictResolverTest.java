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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.MetadataStore;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.SystemTopic;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateDataConflictResolverTest {
    ServiceUnitStateDataConflictResolver strategy = new ServiceUnitStateDataConflictResolver();
    String dst = "dst";
    String src = "src";

    ServiceUnitStateData data(ServiceUnitState state) {
        return new ServiceUnitStateData(state, "broker", 1);
    }

    ServiceUnitStateData data(ServiceUnitState state, String dst) {
        return new ServiceUnitStateData(state, dst, null, 1);
    }

    ServiceUnitStateData data(ServiceUnitState state, String src, String dst) {
        return new ServiceUnitStateData(state, dst, src, 1);
    }

    ServiceUnitStateData data2(ServiceUnitState state) {
        return new ServiceUnitStateData(state, "broker", 2);
    }

    ServiceUnitStateData data2(ServiceUnitState state, String dst) {
        return new ServiceUnitStateData(state, dst, null, 2);
    }

    ServiceUnitStateData data2(ServiceUnitState state, String src, String dst) {
        return new ServiceUnitStateData(state, dst, src, 2);
    }

    @Test
    public void testVersionId(){
        assertTrue(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Assigning, dst, 1),
                new ServiceUnitStateData(Assigning, dst, 1)));

        assertTrue(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Assigning, dst, 1),
                new ServiceUnitStateData(Assigning, dst, 2)));

        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, src, 10),
                new ServiceUnitStateData(Releasing, "broker2", dst, 11)));

        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, src, Long.MAX_VALUE),
                new ServiceUnitStateData(Releasing, "broker2", dst, Long.MAX_VALUE + 1)));

        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, src, Long.MAX_VALUE + 1),
                new ServiceUnitStateData(Releasing, "broker2", dst, Long.MAX_VALUE + 2)));

        assertTrue(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, src, 10),
                new ServiceUnitStateData(Releasing, "broker2", dst, 5)));

        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, src, 10),
                new ServiceUnitStateData(Owned, "broker2", dst, 12)));

    }

    @Test
    public void testStoreType(){
        ServiceUnitStateDataConflictResolver strategy = new ServiceUnitStateDataConflictResolver();
        strategy.setStorageType(SystemTopic);
        assertFalse(strategy.shouldKeepLeft(
                null,
                new ServiceUnitStateData(Owned, dst, 1)));
        assertFalse(strategy.shouldKeepLeft(
                null,
                new ServiceUnitStateData(Owned, dst, 2)));
        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, 1),
                new ServiceUnitStateData(Owned, dst, 3)));

        strategy.setStorageType(MetadataStore);
        assertFalse(strategy.shouldKeepLeft(
                null,
                new ServiceUnitStateData(Owned, dst, 1)));
        assertTrue(strategy.shouldKeepLeft(
                null,
                new ServiceUnitStateData(Owned, dst, 2)));
        assertTrue(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Owned, dst, 1),
                new ServiceUnitStateData(Owned, dst, 3)));
    }

    @Test
    public void testForce(){
        assertFalse(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Init, dst, 1),
                new ServiceUnitStateData(Init, dst, true, 2)));

        assertTrue(strategy.shouldKeepLeft(
                new ServiceUnitStateData(Init, dst, 1),
                new ServiceUnitStateData(Init, dst, true, 1)));
    }

    @Test
    public void testTombstone() {
        assertFalse(strategy.shouldKeepLeft(
                data(Init), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Assigning), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Owned), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Releasing), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Splitting), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Free), null));
        assertFalse(strategy.shouldKeepLeft(
                data(Deleted), null));
    }

    @Test
    public void testTransitionsAndBrokers() {

        assertTrue(strategy.shouldKeepLeft(data(Init), data2(Init)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Free)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Assigning)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Owned)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Releasing)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Splitting)));
        assertFalse(strategy.shouldKeepLeft(data(Init), data2(Deleted)));

        assertTrue(strategy.shouldKeepLeft(data(Assigning), data2(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data2(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data2(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, "dst1"), data2(Owned, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, dst), data2(Owned, src, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigning, dst), data2(Owned, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Assigning, src, dst), data2(Owned, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, src, dst), data2(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning, src, dst), data2(Releasing, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data2(Splitting, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Assigning), data2(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data2(Releasing, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, "dst1"), data2(Releasing, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, dst), data2(Releasing, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Releasing, null, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Releasing, src, null)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Releasing, dst, null)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data2(Releasing, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, "src1", dst), data2(Releasing, "src2", dst)));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Releasing, src, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Releasing, dst, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, src, "dst1"), data2(Splitting, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Owned, "dst1"), data2(Splitting, "dst2")));
        assertFalse(strategy.shouldKeepLeft(data(Owned, dst), data2(Splitting, dst, null)));
        assertFalse(strategy.shouldKeepLeft(data(Owned, src, dst), data2(Splitting, dst, null)));
        assertTrue(strategy.shouldKeepLeft(data(Owned), data2(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Releasing), data2(Init)));
        assertFalse(strategy.shouldKeepLeft(data(Releasing), data2(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "dst1"), data2(Free, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "src1", dst), data2(Free, "src2", dst)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, src, "dst1"), data2(Assigning, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, src, "dst1"), data2(Assigning, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Releasing, "src1", dst), data2(Assigning, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Releasing, src, dst), data2(Assigning, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data2(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data2(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data2(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Releasing), data2(Deleted, dst)));

        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting), data2(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, src, "dst1"), data2(Deleted, src, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, "dst1"), data2(Deleted, "dst2")));
        assertTrue(strategy.shouldKeepLeft(data(Splitting, "src1", dst), data2(Deleted, "src2", dst)));
        assertFalse(strategy.shouldKeepLeft(data(Splitting, dst), data2(Deleted, dst)));
        assertFalse(strategy.shouldKeepLeft(data(Splitting, src, dst), data2(Deleted, src, dst)));

        assertFalse(strategy.shouldKeepLeft(data(Deleted), data2(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Free)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Deleted), data2(Deleted)));

        assertFalse(strategy.shouldKeepLeft(data(Free), data2(Init)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Free)));
        assertFalse(strategy.shouldKeepLeft(data(Free), data2(Assigning)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Assigning, src, dst)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Owned)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Releasing)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Splitting)));
        assertTrue(strategy.shouldKeepLeft(data(Free), data2(Deleted)));
    }
}
