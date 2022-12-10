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

import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "broker")
public class TopBundlesLoadDataTest {

    @Test
    public void testTopBundlesLoadData() {
        List<TopBundlesLoadData.BundleLoadData> bundleStats = new ArrayList<>();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 100;
        bundleStats.add(new TopBundlesLoadData.BundleLoadData("bundle-1", stats1));

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.add(new TopBundlesLoadData.BundleLoadData("bundle-2", stats2));

        NamespaceBundleStats stats3 = new NamespaceBundleStats();
        stats3.msgRateIn = 100000;
        bundleStats.add(new TopBundlesLoadData.BundleLoadData("bundle-3", stats3));

        NamespaceBundleStats stats4 = new NamespaceBundleStats();
        stats4.msgRateIn = 10;
        bundleStats.add(new TopBundlesLoadData.BundleLoadData("bundle-4", stats4));

        TopBundlesLoadData topBundlesLoadData = TopBundlesLoadData.of(bundleStats, 3);
        var top0 = topBundlesLoadData.getTopBundlesLoadData().get(0);
        var top1 = topBundlesLoadData.getTopBundlesLoadData().get(1);
        var top2 = topBundlesLoadData.getTopBundlesLoadData().get(2);

        assertEquals(top0.bundleName(), "bundle-3");
        assertEquals(top1.bundleName(), "bundle-2");
        assertEquals(top2.bundleName(), "bundle-1");
    }
}
