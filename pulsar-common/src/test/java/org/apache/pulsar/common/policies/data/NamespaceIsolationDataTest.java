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

import org.testng.annotations.Test;

public class NamespaceIsolationDataTest {

    @Test
    public void testNamespaceIsolationData() {
        NamespaceIsolationDataImpl n0 = new NamespaceIsolationDataImpl();
        NamespaceIsolationDataImpl n1 = new NamespaceIsolationDataImpl();
        assertNotEquals(new OldPolicies(), n0);
        n0.namespaces = new ArrayList<>();
        n0.primary = new ArrayList<>();
        n0.secondary = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            n0.namespaces.add(String.format("ns%d", i));
            n0.primary.add(String.format("p%d", i));
            n0.secondary.add(String.format("s%d", i));
        }

        assertNotEquals(new NamespaceIsolationDataImpl(), n0);

        n1.namespaces = n0.namespaces;
        n1.primary = n0.primary;
        n1.secondary = n0.secondary;
        assertEquals(n1, n0);

        try {
            n0.validate();
            n1.validate();
            fail("Should not happen");
        } catch (Exception e) {
            // pass
        }

        AutoFailoverPolicyDataImpl policy0 = new AutoFailoverPolicyDataImpl();
        AutoFailoverPolicyDataImpl policy1 = new AutoFailoverPolicyDataImpl();
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
