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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class NamespaceIsolationPoliciesTest {

    private final String defaultJson =
            "{\"policy1\":{\"namespaces\":[\"pulsar/use/test.*\"],\"primary\":[\"prod1-broker[1-3].messaging.use.example.com\"],\"secondary\":[\"prod1-broker.*.use.example.com\"],\"auto_failover_policy\":{\"parameters\":{\"min_limit\":\"3\",\"usage_threshold\":\"100\"},\"policy_type\":\"min_available\"}}}";

    @Test
    public void testJsonSerialization() throws Exception {
        // deserialize JSON string
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();

        // serialize the object to JSON string
        ObjectMapper jsonMapperForWriter = ObjectMapperFactory.create();
        NamespaceIsolationPolicy nsPolicy = policies.getPolicyByName("policy1");
        assertNotNull(nsPolicy);
        List<String> primaryBrokers = nsPolicy.getPrimaryBrokers();
        byte[] primaryBrokersJson = jsonMapperForWriter.writeValueAsBytes(primaryBrokers);
        assertEquals(new String(primaryBrokersJson), "[\"prod1-broker[1-3].messaging.use.example.com\"]");
        List<String> secondaryBrokers = nsPolicy.getSecondaryBrokers();
        byte[] secondaryBrokersJson = jsonMapperForWriter.writeValueAsBytes(secondaryBrokers);
        assertEquals(new String(secondaryBrokersJson), "[\"prod1-broker.*.use.example.com\"]");

        byte[] outJson = jsonMapperForWriter.writeValueAsBytes(policies.getPolicies());
        assertEquals(new String(outJson), this.defaultJson);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("min_limit", "1");
        parameters.put("usage_threshold", "100");

        NamespaceIsolationData nsPolicyData = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList("pulsar/use/other.*"))
                .primary(Collections.singletonList("prod1-broker[4-6].messaging.use.example.com"))
                .secondary(Collections.singletonList("prod1-broker.*.messaging.use.example.com"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters)
                        .build()
                ).build();
        policies.setPolicy("otherPolicy", nsPolicyData);
        byte[] morePolicyJson = jsonMapperForWriter.writeValueAsBytes(policies.getPolicies());
        ObjectMapper jsonParser = ObjectMapperFactory.create();
        Map<String, NamespaceIsolationDataImpl> policiesMap = jsonParser.readValue(morePolicyJson,
                new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
                });
        assertEquals(policiesMap.size(), 2);
    }

    @Test
    public void testDefaultConstructor() throws Exception {
        NamespaceIsolationPolicies policies = new NamespaceIsolationPolicies();
        assertTrue(policies.getPolicies().isEmpty());

        byte[] outJson = ObjectMapperFactory.create().writeValueAsBytes(policies.getPolicies());
        assertEquals(new String(outJson), "{}");
    }

    @Test
    public void testDeletePolicy() throws Exception {
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();
        policies.deletePolicy("non-existing-policy");
        assertFalse(policies.getPolicies().isEmpty());

        policies.deletePolicy("policy1");
        assertTrue(policies.getPolicies().isEmpty());
    }

    @Test
    public void testGetNamespaceIsolationPolicyByName() throws Exception {
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();
        NamespaceIsolationPolicy nsPolicy = policies.getPolicyByName("non-existing-policy");
        assertNull(nsPolicy);
        nsPolicy = policies.getPolicyByName("policy1");
        assertNotNull(nsPolicy);
        assertEquals(new NamespaceIsolationPolicyImpl(policies.getPolicies().get("policy1")), nsPolicy);
    }

    @Test
    public void testGetNamespaceIsolationPolicyByNamespace() throws Exception {
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();
        NamespaceIsolationPolicy nsPolicy = policies.getPolicyByNamespace(NamespaceName.get("no/such/namespace"));
        assertNull(nsPolicy);
        nsPolicy = policies.getPolicyByNamespace(NamespaceName.get("pulsar/use/testns-1"));
        assertNotNull(nsPolicy);
        assertEquals(new NamespaceIsolationPolicyImpl(policies.getPolicies().get("policy1")), nsPolicy);
    }

    @Test
    public void testSetPolicy() throws Exception {
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();
        // set a new policy
        String newPolicyJson = "{\"namespaces\":[\"pulsar/use/TESTNS.*\"],\"primary\":[\"prod1-broker[45].messaging.use.example.com\"],\"secondary\":[\"prod1-broker.*.use.example.com\"],\"auto_failover_policy\":{\"policy_type\":\"min_available\",\"parameters\":{\"min_limit\":2,\"usage_threshold\":80}}}";
        String newPolicyName = "policy2";
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        NamespaceIsolationDataImpl nsPolicyData = jsonMapper.readValue(newPolicyJson.getBytes(),
                NamespaceIsolationDataImpl.class);
        policies.setPolicy(newPolicyName, nsPolicyData);

        assertEquals(policies.getPolicies().size(), 2);
        assertEquals(policies.getPolicyByName(newPolicyName), new NamespaceIsolationPolicyImpl(nsPolicyData));
        assertNotEquals(policies.getPolicyByName("policy1"), policies.getPolicyByName(newPolicyName));
        assertEquals(policies.getPolicyByNamespace(NamespaceName.get("pulsar/use/TESTNS.1")),
                new NamespaceIsolationPolicyImpl(nsPolicyData));
    }

    @SuppressWarnings("unchecked")
    private NamespaceIsolationPolicies getDefaultTestPolicies() throws Exception {
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        return new NamespaceIsolationPolicies(jsonMapper
                .readValue(this.defaultJson.getBytes(), new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
                }));
    }

    @Test
    public void testBrokerAssignment() throws Exception {
        NamespaceIsolationPolicies policies = this.getDefaultTestPolicies();
        NamespaceName ns = NamespaceName.get("pulsar/use/testns-1");
        SortedSet<BrokerStatus> primaryCandidates = new TreeSet<>();
        BrokerStatus primary = BrokerStatus.builder()
                .brokerAddress("prod1-broker1.messaging.use.example.com")
                .active(true)
                .loadFactor(0)
                .build();
        BrokerStatus secondary = BrokerStatus.builder()
                .brokerAddress("prod1-broker4.messaging.use.example.com")
                .active(true)
                .loadFactor(0)
                .build();
        BrokerStatus shared = BrokerStatus.builder()
                .brokerAddress("use.example.com")
                .active(true)
                .loadFactor(0)
                .build();
        SortedSet<BrokerStatus> secondaryCandidates = new TreeSet<>();
        SortedSet<BrokerStatus> sharedCandidates = new TreeSet<>();
        policies.assignBroker(ns, primary, primaryCandidates, secondaryCandidates, sharedCandidates);
        assertEquals(primaryCandidates.size(), 1);
        assertEquals(secondaryCandidates.size(), 0);
        assertEquals(sharedCandidates.size(), 0);
        assertEquals(primary, primaryCandidates.first());
        policies.assignBroker(ns, secondary, primaryCandidates, secondaryCandidates, sharedCandidates);
        assertEquals(primaryCandidates.size(), 1);
        assertEquals(secondaryCandidates.size(), 1);
        assertEquals(sharedCandidates.size(), 0);
        assertEquals(secondary, secondaryCandidates.first());
        policies.assignBroker(NamespaceName.get("pulsar/use1/testns-1"), shared, primaryCandidates, secondaryCandidates,
                sharedCandidates);
        assertEquals(primaryCandidates.size(), 1);
        assertEquals(secondaryCandidates.size(), 1);
        assertEquals(sharedCandidates.size(), 1);
        assertEquals(shared, sharedCandidates.first());
    }
}
