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

import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LoadManagerSharedTest {

    @Test
    public void testRemoveMostServicingBrokersForNamespace() {
        String namespace = "tenant1/ns1";
        String assignedBundle = namespace + "/0x00000000_0x40000000";

        Set<String> candidates = Sets.newHashSet();
        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> map = new ConcurrentOpenHashMap<>();
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 0);

        candidates = Sets.newHashSet("broker1");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1");
        fillBrokerToNamespaceToBundleMap(map, "broker1", namespace, "0x40000000_0x80000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1", "broker2");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker2"));

        candidates = Sets.newHashSet("broker1", "broker2");
        fillBrokerToNamespaceToBundleMap(map, "broker2", namespace, "0x80000000_0xc0000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 2);
        Assert.assertTrue(candidates.contains("broker1"));
        Assert.assertTrue(candidates.contains("broker2"));

        candidates = Sets.newHashSet("broker1", "broker2");
        fillBrokerToNamespaceToBundleMap(map, "broker2", namespace, "0xc0000000_0xd0000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1", "broker2", "broker3");
        fillBrokerToNamespaceToBundleMap(map, "broker3", namespace, "0xd0000000_0xffffffff");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, map);
        Assert.assertEquals(candidates.size(), 2);
        Assert.assertTrue(candidates.contains("broker1"));
        Assert.assertTrue(candidates.contains("broker3"));
    }

    private static void fillBrokerToNamespaceToBundleMap(
            ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> map,
            String broker, String namespace, String bundle) {
        map.computeIfAbsent(broker, k -> new ConcurrentOpenHashMap<>())
                .computeIfAbsent(namespace, k -> new ConcurrentOpenHashSet<>()).add(bundle);
    }

}
