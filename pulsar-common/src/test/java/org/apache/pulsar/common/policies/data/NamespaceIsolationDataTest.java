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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class NamespaceIsolationDataTest {

    @Test
    public void testNamespaceIsolationData() {
        NamespaceIsolationData n0 = NamespaceIsolationData.builder()
                .namespaces(new ArrayList<>())
                .primary(new ArrayList<>())
                .secondary(new ArrayList<>())
                .build();
        assertNotEquals(new OldPolicies(), n0);

        for (int i = 0; i < 5; i++) {
            n0.getNamespaces().add(String.format("ns%d", i));
            n0.getPrimary().add(String.format("p%d", i));
            n0.getSecondary().add(String.format("s%d", i));
        }

        assertNotEquals(NamespaceIsolationData.builder().build(), n0);

        NamespaceIsolationData n1 = NamespaceIsolationData.builder()
                .namespaces(n0.getNamespaces())
                .primary(n0.getPrimary())
                .secondary(n0.getSecondary())
                .build();
        assertEquals(n1, n0);

        try {
            n0.validate();
            n1.validate();
            fail("Should not happen");
        } catch (Exception e) {
            // pass
        }

        Map<String, String> p1parameters = new HashMap<>();
        p1parameters.put("min_limit", "3");
        p1parameters.put("usage_threshold", "10");

        Map<String, String> p2parameters = new HashMap<>();
        p2parameters.put("min_limit", "3");
        p2parameters.put("usage_threshold", "10");

        AutoFailoverPolicyData policy0 = AutoFailoverPolicyData.builder()
                .policyType(AutoFailoverPolicyType.min_available)
                .parameters(p1parameters)
                .build();
        AutoFailoverPolicyData policy1 = AutoFailoverPolicyData.builder()
                .policyType(AutoFailoverPolicyType.min_available)
                .parameters(p1parameters)
                .build();

        n0 = NamespaceIsolationData.builder()
                .namespaces(new ArrayList<>())
                .primary(new ArrayList<>())
                .secondary(new ArrayList<>())
                .autoFailoverPolicy(policy0)
                .build();
        assertNotEquals(new OldPolicies(), n0);

        for (int i = 0; i < 5; i++) {
            n0.getNamespaces().add(String.format("ns%d", i));
            n0.getPrimary().add(String.format("p%d", i));
            n0.getSecondary().add(String.format("s%d", i));
        }

        assertNotEquals(NamespaceIsolationData.builder().build(), n0);

        n1 = NamespaceIsolationData.builder()
                .namespaces(n0.getNamespaces())
                .primary(n0.getPrimary())
                .secondary(n0.getSecondary())
                .autoFailoverPolicy(policy1)
                .build();

        try {
            n0.validate();
            n1.validate();
        } catch (Exception e) {
            fail("Should not happen");
        }

    }
}
