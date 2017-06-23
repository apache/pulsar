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

import java.util.HashMap;

import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.impl.AutoFailoverPolicyFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AutoFailoverPolicyFactoryTest {

    @Test
    public void testAutoFailoverPolicyFactory() {
        try {
            AutoFailoverPolicyFactory.create(new AutoFailoverPolicyData());
            Assert.fail("");
        } catch (IllegalArgumentException e) {
            // Pass
        }
        try {
            AutoFailoverPolicyData afopd = new AutoFailoverPolicyData();
            afopd.policy_type = AutoFailoverPolicyType.min_available;
            afopd.parameters = new HashMap<>();
            afopd.parameters.put("min_limit", "3");
            afopd.parameters.put("usage_threshold", "10");
            AutoFailoverPolicyFactory.create(afopd);
            // Pass
        } catch (IllegalArgumentException e) {
            Assert.fail("Should not happen");
        }

    }

}
