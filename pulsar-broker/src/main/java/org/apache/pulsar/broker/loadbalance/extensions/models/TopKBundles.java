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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Defines the information of top k highest-loaded bundles.
 */
@Getter
@ToString
@EqualsAndHashCode
@Slf4j
public class TopKBundles {

    // temp array for sorting
    private final List<Map.Entry<String, ? extends Comparable>> arr = new ArrayList<>();

    private final TopBundlesLoadData loadData = new TopBundlesLoadData();

    private final PulsarService pulsar;

    private final SimpleResourceAllocationPolicies allocationPolicies;

    public TopKBundles(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.allocationPolicies = new SimpleResourceAllocationPolicies(pulsar);
    }

    /**
     * Update the topK bundles from the input bundleStats.
     *
     * @param bundleStats bundle stats.
     * @param topk        top k bundle stats to select.
     */
    public void update(Map<String, NamespaceBundleStats> bundleStats, int topk) {
        arr.clear();
        try {
            var isLoadBalancerSheddingBundlesWithPoliciesEnabled =
                    pulsar.getConfiguration().isLoadBalancerSheddingBundlesWithPoliciesEnabled();
            for (var etr : bundleStats.entrySet()) {
                String bundle = etr.getKey();
                var stat = etr.getValue();

                // skip zero traffic bundles
                if (stat.msgThroughputIn + stat.msgThroughputOut == 0) {
                    continue;
                }
                // TODO: do not filter system topic while shedding
                if (NamespaceService.isSystemServiceNamespace(NamespaceBundle.getBundleNamespace(bundle))) {
                    continue;
                }
                if (!isLoadBalancerSheddingBundlesWithPoliciesEnabled && hasPolicies(bundle)) {
                    continue;
                }
                arr.add(etr);
            }
            var topKBundlesLoadData = loadData.getTopBundlesLoadData();
            topKBundlesLoadData.clear();
            if (arr.isEmpty()) {
                return;
            }
            topk = Math.min(topk, arr.size());
            partitionSort(arr, topk);

            for (int i = topk - 1; i >= 0; i--) {
                var etr = arr.get(i);
                topKBundlesLoadData.add(
                        new TopBundlesLoadData.BundleLoadData(etr.getKey(), (NamespaceBundleStats) etr.getValue()));
            }
        } finally {
            arr.clear();
        }
    }

    public static void partitionSort(List<Map.Entry<String, ? extends Comparable>> arr, int k) {
        int start = 0;
        int end = arr.size() - 1;
        int target = k - 1;
        while (start < end) {
            int lo = start;
            int hi = end;
            int mid = lo;
            var pivot = arr.get(hi).getValue();
            while (mid <= hi) {
                int cmp = pivot.compareTo(arr.get(mid).getValue());
                if (cmp < 0) {
                    var tmp = arr.get(lo);
                    arr.set(lo++, arr.get(mid));
                    arr.set(mid++, tmp);
                } else if (cmp > 0) {
                    var tmp = arr.get(mid);
                    arr.set(mid, arr.get(hi));
                    arr.set(hi--, tmp);
                } else {
                    mid++;
                }
            }
            if (lo <= target && target < mid) {
                end = lo;
                break;
            }
            if (target < lo) {
                end = lo - 1;
            } else {
                start = mid;
            }
        }
        Collections.sort(arr.subList(0, end), (a, b) -> b.getValue().compareTo(a.getValue()));
    }

    private boolean hasPolicies(String bundle) {
        NamespaceName namespace = NamespaceName.get(LoadManagerShared.getNamespaceNameFromBundleName(bundle));
        if (allocationPolicies.areIsolationPoliciesPresent(namespace)) {
            return true;
        }

        try {
            var antiAffinityGroupOptional =
                    LoadManagerShared.getNamespaceAntiAffinityGroup(pulsar, namespace.toString());
            if (antiAffinityGroupOptional.isPresent()) {
                return true;
            }
        } catch (MetadataStoreException e) {
            log.error("Failed to get localPolicies for bundle:{}.", bundle, e);
            throw new RuntimeException(e);
        }
        return false;
    }
}
