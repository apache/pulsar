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

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AutoFailoverPolicyDataTest {

    @Test
    public void testAutoFailoverPolicyData() {
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
                .parameters(p2parameters)
                .build();

        try {
            policy0.validate();
            policy1.validate();
        } catch (Exception e) {
            fail("Should not happen");
        }
        assertEquals(policy1, policy0);
        p1parameters.put("min_limit", "5");
        assertNotEquals(policy1, policy0);
        assertNotEquals(new OldPolicies(), policy1);
    }
}
