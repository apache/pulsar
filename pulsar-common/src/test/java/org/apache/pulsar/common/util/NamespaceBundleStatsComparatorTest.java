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
package org.apache.pulsar.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage.ResourceType;
import org.testng.annotations.Test;

public class NamespaceBundleStatsComparatorTest {

    @Test
    public void keepsEqualLoadBundlesAddressable() {
        NamespaceBundleStats firstStats = new NamespaceBundleStats();
        NamespaceBundleStats secondStats = new NamespaceBundleStats();
        Map<String, NamespaceBundleStats> stats = new HashMap<>();
        stats.put("bundle-a", firstStats);
        stats.put("bundle-b", secondStats);

        NamespaceBundleStatsComparator comparator =
                new NamespaceBundleStatsComparator(stats, ResourceType.CPU);
        TreeMap<String, NamespaceBundleStats> sortedStats = new TreeMap<>(comparator);
        sortedStats.putAll(stats);

        assertThat(comparator.compare("bundle-a", "bundle-a")).isZero();
        assertThat(sortedStats).containsOnlyKeys("bundle-a", "bundle-b");
        assertThat(sortedStats.get("bundle-a")).isSameAs(firstStats);
        assertThat(sortedStats.get("bundle-b")).isSameAs(secondStats);
    }
}
