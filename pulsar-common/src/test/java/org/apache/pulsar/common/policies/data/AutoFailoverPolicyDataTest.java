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

import java.util.HashMap;

import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.testng.annotations.Test;

public class AutoFailoverPolicyDataTest {

    @Test
    public void testAutoFailoverPolicyData() {
        AutoFailoverPolicyData policy0 = new AutoFailoverPolicyData();
        AutoFailoverPolicyData policy1 = new AutoFailoverPolicyData();
        policy0.policy_type = AutoFailoverPolicyType.min_available;
        policy0.parameters = new HashMap<>();
        policy0.parameters.put("min_limit", "3");
        policy0.parameters.put("usage_threshold", "10");
        policy1.policy_type = AutoFailoverPolicyType.min_available;
        policy1.parameters = new HashMap<>();
        policy1.parameters.put("min_limit", "3");
        policy1.parameters.put("usage_threshold", "10");
        try {
            policy0.validate();
            policy1.validate();
        } catch (Exception e) {
            fail("Should not happen");
        }
        assertTrue(policy0.equals(policy1));
        policy1.parameters.put("min_limit", "5");
        assertFalse(policy0.equals(policy1));
        assertFalse(policy1.equals(new OldPolicies()));
    }
}
