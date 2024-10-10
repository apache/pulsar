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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.policies.IsolationPoliciesHelper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

/**
 * Unit test for {@link BrokerIsolationPoliciesFilter}.
 */
@Test(groups = "broker")
public class BrokerIsolationPoliciesFilterTest {

    /**
     * It verifies namespace-isolation policies with primary and secondary brokers.
     *
     * usecase:
     *
     * <pre>
     *  1. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 1
     *     a. available-brokers: broker1, broker2, broker3 => result: broker1
     *     b. available-brokers: broker2, broker3          => result: broker2
     *     c. available-brokers: broker3                   => result: NULL
     *  2. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 2
     *     a. available-brokers: broker1, broker2, broker3 => result: broker1, broker2
     *     b. available-brokers: broker2, broker3          => result: broker2
     *     c. available-brokers: broker3                   => result: NULL
     * </pre>
     */
    @Test
    public void testFilterWithNamespaceIsolationPoliciesForPrimaryAndSecondaryBrokers()
            throws IllegalAccessException, BrokerFilterException, ExecutionException, InterruptedException {
        var namespace = "my-tenant/my-ns";
        NamespaceName namespaceName = NamespaceName.get(namespace);

        var policies = mock(SimpleResourceAllocationPolicies.class);

        // 1. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 1
        setIsolationPolicies(policies, namespaceName, Set.of("broker1"), Set.of("broker2"), Set.of("broker3"), 1);
        IsolationPoliciesHelper isolationPoliciesHelper = new IsolationPoliciesHelper(policies);

        BrokerIsolationPoliciesFilter filter = new BrokerIsolationPoliciesFilter(isolationPoliciesHelper);

        // a. available-brokers: broker1, broker2, broker3 => result: broker1
        Map<String, BrokerLookupData> result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(),
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker1:8080"));

        // b. available-brokers: broker2, broker3          => result: broker2
        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker2:8080"));

        // c. available-brokers: broker3                   => result: NULL
        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertTrue(result.isEmpty());

        // 2. Namespace: primary=broker1, secondary=broker2, shared=broker3, min_limit = 2
        setIsolationPolicies(policies, namespaceName, Set.of("broker1"), Set.of("broker2"), Set.of("broker3"), 2);

        // a. available-brokers: broker1, broker2, broker3 => result: broker1, broker2
        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(),
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker1:8080", "broker2:8080"));

        // b. available-brokers: broker2, broker3          => result: broker2
        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker2:8080"));

        // c. available-brokers: broker3                   => result: NULL
        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker3:8080", getLookupData())), namespaceName, getContext()).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterWithPersistentOrNonPersistentDisabled()
            throws IllegalAccessException, BrokerFilterException, ExecutionException, InterruptedException {
        var namespace = "my-tenant/my-ns";
        NamespaceName namespaceName = NamespaceName.get(namespace);
        NamespaceBundle namespaceBundle = mock(NamespaceBundle.class);
        doReturn(true).when(namespaceBundle).hasNonPersistentTopic();
        doReturn(namespaceName).when(namespaceBundle).getNamespaceObject();

        var policies = mock(SimpleResourceAllocationPolicies.class);
        doReturn(CompletableFuture.completedFuture(false))
                .when(policies).areIsolationPoliciesPresentAsync(eq(namespaceName));
        doReturn(true).when(policies).isSharedBroker(any());
        IsolationPoliciesHelper isolationPoliciesHelper = new IsolationPoliciesHelper(policies);

        BrokerIsolationPoliciesFilter filter = new BrokerIsolationPoliciesFilter(isolationPoliciesHelper);



        Map<String, BrokerLookupData> result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(),
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceBundle, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker1:8080", "broker2:8080", "broker3:8080"));


        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(true, false),
                "broker2:8080", getLookupData(true, false),
                "broker3:8080", getLookupData())), namespaceBundle, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker3:8080"));

        doReturn(false).when(namespaceBundle).hasNonPersistentTopic();

        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(),
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceBundle, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker1:8080", "broker2:8080", "broker3:8080"));

        result = filter.filterAsync(new HashMap<>(Map.of(
                "broker1:8080", getLookupData(false, true),
                "broker2:8080", getLookupData(),
                "broker3:8080", getLookupData())), namespaceBundle, getContext()).get();
        assertEquals(result.keySet(), Set.of("broker2:8080", "broker3:8080"));
    }

    private void setIsolationPolicies(SimpleResourceAllocationPolicies policies,
                                      NamespaceName namespaceName,
                                      Set<String> primary,
                                      Set<String> secondary,
                                      Set<String> shared,
                                      int min_limit) {
        reset(policies);
        doReturn(CompletableFuture.completedFuture(true))
                .when(policies).areIsolationPoliciesPresentAsync(eq(namespaceName));
        doReturn(false).when(policies).isPrimaryBroker(eq(namespaceName), any());
        doReturn(false).when(policies).isSecondaryBroker(eq(namespaceName), any());
        doReturn(false).when(policies).isSharedBroker(any());

        primary.forEach(broker -> {
            doReturn(true).when(policies).isPrimaryBroker(eq(namespaceName), eq(broker));
        });

        secondary.forEach(broker -> {
            doReturn(true).when(policies).isSecondaryBroker(eq(namespaceName), eq(broker));
        });

        shared.forEach(broker -> {
            doReturn(true).when(policies).isSharedBroker(eq(broker));
        });

        doAnswer(invocationOnMock -> {
            Integer totalPrimaryCandidates = invocationOnMock.getArgument(1, Integer.class);
            return totalPrimaryCandidates < min_limit;
        }).when(policies).shouldFailoverToSecondaries(eq(namespaceName), anyInt());
    }

    public BrokerLookupData getLookupData() {
        return getLookupData(true, true);
    }

    public BrokerLookupData getLookupData(boolean persistentTopicsEnabled,
                                          boolean nonPersistentTopicsEnabled) {
        String webServiceUrl = "http://localhost:8080";
        String webServiceUrlTls = "https://localhoss:8081";
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarServiceUrlTls = "pulsar+ssl://localhost:6651";
        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        Map<String, String> protocols = new HashMap<>(){{
            put("kafka", "9092");
        }};
        return new BrokerLookupData(
                webServiceUrl, webServiceUrlTls, pulsarServiceUrl,
                pulsarServiceUrlTls, advertisedListeners, protocols,
                persistentTopicsEnabled, nonPersistentTopicsEnabled,
                ExtensibleLoadManagerImpl.class.getName(), System.currentTimeMillis(), "3.0.0", Collections.emptyMap());
    }

    public LoadManagerContext getContext() {
        LoadManagerContext mockContext = mock(LoadManagerContext.class);
        doReturn(new ServiceConfiguration()).when(mockContext).brokerConfiguration();
        return mockContext;
    }
}
