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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicyImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LoadManagerSharedTest {

    @Test
    public void testRemoveMostServicingBrokersForNamespace() {
        String namespace = "tenant1/ns1";
        String assignedBundle = namespace + "/0x00000000_0x40000000";

        Set<String> candidates = new HashSet<>();
        final var cache = new BundleRangeCache();
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 0);

        candidates = Sets.newHashSet("broker1");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1");
        cache.add("broker1", namespace, "0x40000000_0x80000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1", "broker2");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker2"));

        candidates = Sets.newHashSet("broker1", "broker2");
        cache.add("broker2", namespace, "0x80000000_0xc0000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 2);
        Assert.assertTrue(candidates.contains("broker1"));
        Assert.assertTrue(candidates.contains("broker2"));

        candidates = Sets.newHashSet("broker1", "broker2");
        cache.add("broker2", namespace, "0xc0000000_0xd0000000");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 1);
        Assert.assertTrue(candidates.contains("broker1"));

        candidates = Sets.newHashSet("broker1", "broker2", "broker3");
        cache.add("broker3", namespace, "0xd0000000_0xffffffff");
        LoadManagerShared.removeMostServicingBrokersForNamespace(assignedBundle, candidates, cache);
        Assert.assertEquals(candidates.size(), 2);
        Assert.assertTrue(candidates.contains("broker1"));
        Assert.assertTrue(candidates.contains("broker3"));
    }

    private static final LoadManagerShared.BrokerTopicLoadingPredicate ALL_TOPICS =
            new LoadManagerShared.BrokerTopicLoadingPredicate() {
                @Override
                public boolean isEnablePersistentTopics(String brokerId) {
                    return true;
                }

                @Override
                public boolean isEnableNonPersistentTopics(String brokerId) {
                    return true;
                }
            };

    private static SimpleResourceAllocationPolicies policiesWithPrimary(NamespaceName namespace,
                                                                        String... primary) {
        NamespaceIsolationPolicyImpl policy = new NamespaceIsolationPolicyImpl(NamespaceIsolationData.builder()
                .namespaces(List.of(namespace.toString()))
                .primary(List.of(primary))
                .secondary(List.of())
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(Map.of("min_limit", "1", "usage_threshold", "100"))
                        .build())
                .build());
        SimpleResourceAllocationPolicies policies = mock(SimpleResourceAllocationPolicies.class, CALLS_REAL_METHODS);
        doReturn(true).when(policies).areIsolationPoliciesPresent(namespace);
        doReturn(CompletableFuture.completedFuture(true)).when(policies).areIsolationPoliciesPresentAsync(namespace);
        doReturn(Optional.of(policy)).when(policies).getNamespaceIsolationPolicy(namespace);
        doReturn(false).when(policies).isSharedBroker(any());
        return policies;
    }

    private static void assertPlacement(SimpleResourceAllocationPolicies policies, NamespaceName namespace,
                                        Set<String> availableBrokers, Set<String> expected) throws Exception {
        Set<String> candidates = new HashSet<>();
        LoadManagerShared.applyNamespacePolicies(namespace, policies, candidates, availableBrokers, ALL_TOPICS);
        Assert.assertEquals(candidates, expected);
        Assert.assertEquals(LoadManagerShared.applyNamespacePoliciesAsync(namespace, policies, availableBrokers,
                ALL_TOPICS).get(), expected);
    }

    @Test
    public void testApplyNamespacePoliciesWithBrokerIdPolicy() throws Exception {
        NamespaceName namespace = NamespaceName.get("tenant1/ns1");
        assertPlacement(policiesWithPrimary(namespace, "broker1:8080"), namespace,
                Set.of("broker1:8080", "broker1:8081", "broker2:8080"), Set.of("broker1:8080"));
    }

    @Test
    public void testApplyNamespacePoliciesKeepsHostRegexSemantics() throws Exception {
        NamespaceName namespace = NamespaceName.get("tenant1/ns1");
        // must not match broker2 through the ":8081" port suffix of its broker id
        assertPlacement(policiesWithPrimary(namespace, "broker.*[13]"), namespace,
                Set.of("broker1:8080", "broker2:8081", "broker3:8080"), Set.of("broker1:8080", "broker3:8080"));
        // the last segment of an IPv6 host must not be taken for a port
        assertPlacement(policiesWithPrimary(namespace, "2001:db8::1"), namespace,
                Set.of("2001:db8::1:1234:8080", "2001:db8::1:8080"), Set.of("2001:db8::1:8080"));
    }
}
