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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopKBundlesTest {
    private PulsarService pulsar;
    private ServiceConfiguration configuration;
    private NamespaceResources.IsolationPolicyResources isolationPolicyResources;
    private PulsarResources pulsarResources;
    private LocalPoliciesResources localPoliciesResources;

    String bundle1 = "my-tenant/my-namespace1/0x00000000_0x0FFFFFFF";
    String bundle2 = "my-tenant/my-namespace2/0x00000000_0x0FFFFFFF";
    String bundle3 = "my-tenant/my-namespace3/0x00000000_0x0FFFFFFF";
    String bundle4 = "my-tenant/my-namespace4/0x00000000_0x0FFFFFFF";

    @BeforeMethod
    public void init() throws MetadataStoreException {
        pulsar = mock(PulsarService.class);
        configuration = new ServiceConfiguration();
        doReturn(configuration).when(pulsar).getConfiguration();
        configuration.setLoadBalancerSheddingBundlesWithPoliciesEnabled(false);
        pulsarResources = mock(PulsarResources.class);
        var namespaceResources = mock(NamespaceResources.class);

        isolationPolicyResources = mock(NamespaceResources.IsolationPolicyResources.class);
        doReturn(pulsarResources).when(pulsar).getPulsarResources();
        doReturn(namespaceResources).when(pulsarResources).getNamespaceResources();
        doReturn(isolationPolicyResources).when(namespaceResources).getIsolationPolicies();
        doReturn(Optional.empty()).when(isolationPolicyResources).getIsolationDataPolicies(any());

        localPoliciesResources = mock(LocalPoliciesResources.class);
        doReturn(localPoliciesResources).when(pulsarResources).getLocalPolicies();
        doReturn(Optional.empty()).when(localPoliciesResources).getLocalPolicies(any());

    }
    @Test
    public void testTopBundlesLoadData() {
        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles(pulsar);
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 100000;
        bundleStats.put(bundle1, stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 500;
        bundleStats.put(bundle2, stats2);

        NamespaceBundleStats stats3 = new NamespaceBundleStats();
        stats3.msgRateIn = 10000;
        bundleStats.put(bundle3, stats3);

        NamespaceBundleStats stats4 = new NamespaceBundleStats();
        stats4.msgRateIn = 0;
        bundleStats.put(bundle4, stats4);

        topKBundles.update(bundleStats, 3);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        var top1 = topKBundles.getLoadData().getTopBundlesLoadData().get(1);
        var top2 = topKBundles.getLoadData().getTopBundlesLoadData().get(2);

        assertEquals(top0.bundleName(), bundle2);
        assertEquals(top1.bundleName(), bundle3);
        assertEquals(top2.bundleName(), bundle1);
    }

    @Test
    public void testSystemNamespace() {
        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles(pulsar);
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put("pulsar/system/0x00000000_0x0FFFFFFF", stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put(bundle1, stats2);

        topKBundles.update(bundleStats, 2);

        assertEquals(topKBundles.getLoadData().getTopBundlesLoadData().size(), 1);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        assertEquals(top0.bundleName(), bundle1);
    }


    private  void setAntiAffinityGroup() throws MetadataStoreException {
        LocalPolicies localPolicies = new LocalPolicies(null, null, "namespaceAntiAffinityGroup");
        NamespaceName namespace = NamespaceName.get(LoadManagerShared.getNamespaceNameFromBundleName(bundle2));
        doReturn(Optional.of(localPolicies)).when(localPoliciesResources).getLocalPolicies(eq(namespace));
    }

    private void setIsolationPolicy() throws MetadataStoreException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("min_limit", "3");
        parameters.put("usage_threshold", "90");
        var policyData = Map.of("policy", (NamespaceIsolationDataImpl)
                NamespaceIsolationData.builder()
                        .namespaces(Collections.singletonList("my-tenant/my-namespace1.*"))
                        .primary(Collections.singletonList("prod1-broker[1-3].messaging.use.example.com"))
                        .secondary(Collections.singletonList("prod1-broker.*.use.example.com"))
                        .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                                .policyType(AutoFailoverPolicyType.min_available)
                                .parameters(parameters)
                                .build()
                        ).build());

        NamespaceIsolationPolicies policies = new NamespaceIsolationPolicies(policyData);
        doReturn(Optional.of(policies)).when(isolationPolicyResources).getIsolationDataPolicies(any());
    }

    @Test
    public void testIsolationPolicy() throws MetadataStoreException {

        setIsolationPolicy();

        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles(pulsar);
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put(bundle1, stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put(bundle2, stats2);

        topKBundles.update(bundleStats, 2);
        assertEquals(topKBundles.getLoadData().getTopBundlesLoadData().size(), 1);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        assertEquals(top0.bundleName(), bundle2);
    }


    @Test
    public void testAntiAffinityGroupPolicy() throws MetadataStoreException {

        setAntiAffinityGroup();

        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles(pulsar);
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put(bundle1, stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put(bundle2, stats2);

        topKBundles.update(bundleStats, 2);
        assertEquals(topKBundles.getLoadData().getTopBundlesLoadData().size(), 1);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        assertEquals(top0.bundleName(), bundle1);

    }

    @Test
    public void testLoadBalancerSheddingBundlesWithPoliciesEnabledConfig() throws MetadataStoreException {

        setIsolationPolicy();
        setAntiAffinityGroup();

        configuration.setLoadBalancerSheddingBundlesWithPoliciesEnabled(true);

        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        var topKBundles = new TopKBundles(pulsar);
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 500;
        bundleStats.put(bundle1, stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put(bundle2, stats2);

        topKBundles.update(bundleStats, 2);

        assertEquals(topKBundles.getLoadData().getTopBundlesLoadData().size(), 2);
        var top0 = topKBundles.getLoadData().getTopBundlesLoadData().get(0);
        var top1 = topKBundles.getLoadData().getTopBundlesLoadData().get(1);

        assertEquals(top0.bundleName(), bundle1);
        assertEquals(top1.bundleName(), bundle2);

        configuration.setLoadBalancerSheddingBundlesWithPoliciesEnabled(false);

        topKBundles.update(bundleStats, 2);

        assertEquals(topKBundles.getLoadData().getTopBundlesLoadData().size(), 0);
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

    // Issue https://github.com/apache/pulsar/issues/24754
    @Test
    public void testPartitionSortCompareToContractViolationIssue() {
        Random rnd = new Random(0);
        ArrayList<NamespaceBundleStats> stats = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            NamespaceBundleStats s = new NamespaceBundleStats();
            s.msgThroughputIn = 4 * 75000 * rnd.nextDouble();  // Just above threshold (1e5)
            s.msgThroughputOut = 75000000 - (4 * (75000 * rnd.nextDouble()));
            s.msgRateIn = 4 * 75 * rnd.nextDouble();
            s.msgRateOut = 75000 - (4 * 75 * rnd.nextDouble());
            s.topics = i;
            s.consumerCount = i;
            s.producerCount = 4 * rnd.nextInt(375);
            s.cacheSize = 75000000 - (rnd.nextInt(4 * 75000));
            stats.add(s);
        }
        List<Map.Entry<String, ? extends Comparable>> bundleEntries = new ArrayList<>();

        for (NamespaceBundleStats s : stats) {
            bundleEntries.add(Map.entry("bundle-" + s.msgThroughputIn, s));
        }
        TopKBundles.partitionSort(bundleEntries, 100);
    }
}
