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
package org.apache.pulsar.broker.loadbalance.extensions.models;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopKBundlesTest {

    @Test
    public void testTopBundlesLoadData() {
        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put("bundle-1", stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put("bundle-2", stats2);

        NamespaceBundleStats stats3 = new NamespaceBundleStats();
        stats3.msgRateIn = 100000;
        bundleStats.put("bundle-3", stats3);

        NamespaceBundleStats stats4 = new NamespaceBundleStats();
        stats4.msgRateIn = 0;
        bundleStats.put("bundle-4", stats4);

        topKBundles.update(bundleStats, 3);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        var top1 = topKBundles.getLoadData().getTopBundlesLoadData().get(1);
        var top2 = topKBundles.getLoadData().getTopBundlesLoadData().get(2);

        assertEquals(top0.bundleName(), "bundle-3");
        assertEquals(top1.bundleName(), "bundle-2");
        assertEquals(top2.bundleName(), "bundle-1");
    }

    @Test
    public void testSystemNamespace() {
        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put("pulsar/system/bundle-1", stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put("pulsar/system/bundle-2", stats2);

        topKBundles.update(bundleStats, 2);
        assertTrue(topKBundles.getLoadData().getTopBundlesLoadData().isEmpty());
    }


    @Test
    public void testPartitionSort() {

        Random rand = new Random();
        List<Map.Entry<String, ? extends Comparable>> actual = new ArrayList<>();
        List<Map.Entry<String, ? extends Comparable>> expected = new ArrayList<>();

        for (int j = 0; j < 100; j++) {
            Map<String, Integer> map = new HashMap<>();
            int max = rand.nextInt(10) + 1;
            for (int i = 0; i < max; i++) {
                int val = rand.nextInt(max);
                map.put("" + i, val);
            }
            actual.clear();
            expected.clear();
            for (var etr : map.entrySet()) {
                actual.add(etr);
                expected.add(etr);
            }
            int topk = rand.nextInt(max) + 1;
            TopKBundles.partitionSort(actual, topk);
            Collections.sort(expected, (a, b) -> b.getValue().compareTo(a.getValue()));
            String errorMsg = null;
            for (int i = 0; i < topk; i++) {
                Integer l = (Integer) actual.get(i).getValue();
                Integer r = (Integer) expected.get(i).getValue();
                if (!l.equals(r)) {
                    errorMsg = String.format("Diff found at i=%d, %d != %d, actual:%s, expected:%s",
                            i, l, r, actual, expected);
                }
                assertNull(errorMsg);
            }
        }
    }
}
