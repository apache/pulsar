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
package org.apache.pulsar.common.policies.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyDataImpl;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.OldPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class NamespaceIsolationPolicyImplTest {
    private final String defaultPolicyJson = "{\"namespaces\":[\"pulsar/use/test.*\"],\"primary\":[\"prod1-broker[1-3].messaging.use.example.com\"],\"secondary\":[\"prod1-broker.*.use.example.com\"],\"auto_failover_policy\":{\"policy_type\":\"min_available\",\"parameters\":{\"min_limit\":\"3\",\"usage_threshold\":\"90\"}}}";

    private NamespaceIsolationPolicyImpl getDefaultPolicy() throws Exception {
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        return new NamespaceIsolationPolicyImpl(
                jsonMapper.readValue(this.defaultPolicyJson.getBytes(), NamespaceIsolationData.class));
    }

    @Test
    public void testConstructor() throws Exception {
        NamespaceIsolationPolicyImpl defaultPolicy = this.getDefaultPolicy();

        Map<String, String> parameters = new HashMap<>();
        parameters.put("min_limit", "3");
        parameters.put("usage_threshold", "90");

        NamespaceIsolationData policyData = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList("pulsar/use/test.*"))
                .primary(Collections.singletonList("prod1-broker[1-3].messaging.use.example.com"))
                .secondary(Collections.singletonList("prod1-broker.*.use.example.com"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters)
                        .build()
                ).build();

        NamespaceIsolationPolicyImpl newPolicy = new NamespaceIsolationPolicyImpl(policyData);
        assertEquals(newPolicy, defaultPolicy);
        parameters.put("usage_threshold", "80");
        newPolicy = new NamespaceIsolationPolicyImpl(policyData);
        assertNotEquals(newPolicy, defaultPolicy);
        assertNotEquals(new OldPolicies(), newPolicy);
    }

    @Test
    public void testGetPrimaryBrokers() throws Exception {
        List<String> primaryBrokers = this.getDefaultPolicy().getPrimaryBrokers();
        assertEquals(primaryBrokers.size(), 1);
        assertEquals(primaryBrokers.get(0), "prod1-broker[1-3].messaging.use.example.com");
    }

    @Test
    public void testGetSecondaryBrokers() throws Exception {
        List<String> secondaryBrokers = this.getDefaultPolicy().getSecondaryBrokers();
        assertEquals(secondaryBrokers.size(), 1);
        assertEquals(secondaryBrokers.get(0), "prod1-broker.*.use.example.com");
    }

    @Test
    public void testIsPrimaryOrSecondaryBroker() throws Exception {
        NamespaceIsolationPolicyImpl defaultPolicy = this.getDefaultPolicy();
        assertTrue(defaultPolicy.isPrimaryBroker("prod1-broker2.messaging.use.example.com"));
        assertFalse(defaultPolicy.isPrimaryBroker("prod1-broker5.messaging.use.example.com"));
        assertTrue(defaultPolicy.isSecondaryBroker("prod1-broker5.messaging.use.example.com"));
        assertFalse(defaultPolicy.isSecondaryBroker("broker-X.messaging.use.example.com"));
    }

    @Test
    public void testFindBrokers() throws Exception {
        NamespaceIsolationPolicyImpl defaultPolicy = this.getDefaultPolicy();
        List<URL> brokers = new ArrayList<URL>();
        for (int i = 0; i < 10; i++) {
            String broker = String.format("prod1-broker%d.messaging.use.example.com", i);
            brokers.add(new URL(String.format("http://%s:8080", broker)));
        }
        List<URL> otherBrokers = new ArrayList<URL>();
        for (int i = 0; i < 10; i++) {
            String broker = String.format("prod1-broker%d.messaging.usw.example.com", i);
            brokers.add(new URL(String.format("http://%s:8080", broker)));
        }
        List<URL> primaryBrokers = defaultPolicy.findPrimaryBrokers(brokers, NamespaceName.get("pulsar/use/testns-1"));
        assertEquals(primaryBrokers.size(), 3);
        for (URL primaryBroker : primaryBrokers) {
            assertTrue(primaryBroker.getHost().matches("prod1-broker[1-3].messaging.use.example.com"));
        }
        primaryBrokers = defaultPolicy.findPrimaryBrokers(otherBrokers, NamespaceName.get("pulsar/use/testns-1"));
        assertTrue(primaryBrokers.isEmpty());
        try {
            primaryBrokers = defaultPolicy.findPrimaryBrokers(brokers, NamespaceName.get("no/such/namespace"));
        } catch (IllegalArgumentException iae) {
            // OK
        }
        List<URL> secondaryBrokers = defaultPolicy.findSecondaryBrokers(brokers,
                NamespaceName.get("pulsar/use/testns-1"));
        assertEquals(secondaryBrokers.size(), 10);
        for (URL secondaryBroker : secondaryBrokers) {
            assertTrue(secondaryBroker.getHost().matches("prod1-broker.*.messaging.use.example.com"));
        }
        secondaryBrokers = defaultPolicy.findSecondaryBrokers(otherBrokers, NamespaceName.get("pulsar/use/testns-1"));
        assertTrue(secondaryBrokers.isEmpty());
        try {
            secondaryBrokers = defaultPolicy.findSecondaryBrokers(brokers, NamespaceName.get("no/such/namespace"));
        } catch (IllegalArgumentException iae) {
            // OK
        }
    }

    @Test
    public void testShouldFailover() throws Exception {
        NamespaceIsolationPolicyImpl defaultPolicy = this.getDefaultPolicy();
        List<BrokerStatus> brokerStatus = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BrokerStatus status = BrokerStatus.builder()
                    .brokerAddress(String.format("broker-%s", i))
                    .active(true)
                    .loadFactor(i * 10)
                    .build();
            brokerStatus.add(status);
        }
        assertFalse(defaultPolicy.shouldFailover(new TreeSet<>(brokerStatus)));
        for (int i = 0; i < 8; i++) {
            brokerStatus.set(i, BrokerStatus.builder()
                    .brokerAddress(brokerStatus.get(i).getBrokerAddress())
                    .active(false)
                    .loadFactor(brokerStatus.get(i).getLoadFactor())
                    .build());
        }
        assertTrue(defaultPolicy.shouldFailover(new TreeSet<>(brokerStatus)));
        brokerStatus.set(7, BrokerStatus.builder()
                .brokerAddress(brokerStatus.get(7).getBrokerAddress())
                .active(true)
                .loadFactor(brokerStatus.get(7).getLoadFactor())
                .build());
        assertTrue(defaultPolicy.shouldFailover(new TreeSet<>(brokerStatus)));
        brokerStatus.set(9, BrokerStatus.builder()
                .brokerAddress(brokerStatus.get(9).getBrokerAddress())
                .active(brokerStatus.get(9).isActive())
                .loadFactor(80)
                .build());
        assertFalse(defaultPolicy.shouldFailover(new TreeSet<>(brokerStatus)));

        brokerStatus = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            BrokerStatus status = BrokerStatus.builder().brokerAddress(String.format("broker-%d", 2 * i))
                    .active(true)
                    .loadFactor(i * 20)
                    .build();
            brokerStatus.add(status);
            status = BrokerStatus.builder()
                    .brokerAddress(String.format("broker-%d", 2 * i + 1))
                    .active(true)
                    .loadFactor(i * 20)
                    .build();
            brokerStatus.add(status);
        }
        assertEquals(brokerStatus.size(), 10);
    }

    @Test
    public void testGetAvailablePrimaryBrokers() throws Exception {
        NamespaceIsolationPolicyImpl defaultPolicy = this.getDefaultPolicy();
        SortedSet<BrokerStatus> brokerStatus = new TreeSet<>();
        SortedSet<BrokerStatus> expectedAvailablePrimaries = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            BrokerStatus status = BrokerStatus.builder()
                    .brokerAddress(String.format("prod1-broker%d.messaging.use.example.com", i))
                    .active(i % 2 == 0)
                    .loadFactor(i * 10)
                    .build();
            brokerStatus.add(status);
            if (i % 2 == 0) {
                expectedAvailablePrimaries.add(status);
            }
        }

        SortedSet<BrokerStatus> availablePrimaries = defaultPolicy.getAvailablePrimaryBrokers(brokerStatus);
        assertEquals(expectedAvailablePrimaries.size(), availablePrimaries.size());
        for (BrokerStatus bs : availablePrimaries) {
            if (!expectedAvailablePrimaries.contains(bs)) {
                fail("Should not happen");
            }
        }

    }

}
