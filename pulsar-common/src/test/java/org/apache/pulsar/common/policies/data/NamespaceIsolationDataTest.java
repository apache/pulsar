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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.testng.annotations.Test;

public class NamespaceIsolationDataTest {

    @Test
    public void testNamespaceIsolationData() {
        NamespaceIsolationData n0 = new NamespaceIsolationData();
        NamespaceIsolationData n1 = new NamespaceIsolationData();
        assertFalse(n0.equals(new OldPolicies()));
        n0.namespaces = new ArrayList<>();
        n0.primary = new ArrayList<>();
        n0.secondary = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            n0.namespaces.add(String.format("ns%d", i));
            n0.primary.add(String.format("p%d", i));
            n0.secondary.add(String.format("s%d", i));
        }

        assertFalse(n0.equals(new NamespaceIsolationData()));

        n1.namespaces = n0.namespaces;
        n1.primary = n0.primary;
        n1.secondary = n0.secondary;
        assertTrue(n0.equals(n1));

        try {
            n0.validate();
            n1.validate();
            fail("Should not happen");
        } catch (Exception e) {
            // pass
        }

        AutoFailoverPolicyData policy0 = new AutoFailoverPolicyData();
        AutoFailoverPolicyData policy1 = new AutoFailoverPolicyData();
        policy0.policy_type = AutoFailoverPolicyType.min_available;
        policy0.parameters = new HashMap<>();
        policy0.parameters.put("min_limit", "3");
        policy0.parameters.put("usage_threshold", "10");
        policy1.policy_type = AutoFailoverPolicyType.min_available;
        policy1.parameters = new HashMap<>(policy0.parameters);

        n0.auto_failover_policy = policy0;
        n1.auto_failover_policy = policy1;

        try {
            n0.validate();
            n1.validate();
        } catch (Exception e) {
            fail("Should not happen");
        }

    }
}
