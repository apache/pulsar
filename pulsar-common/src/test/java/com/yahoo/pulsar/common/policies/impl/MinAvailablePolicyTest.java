/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.policies.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.pulsar.common.policies.data.OldPolicies;

public class MinAvailablePolicyTest {

    @Test
    public void testMinAvailablePolicty() {
        MinAvailablePolicy m = new MinAvailablePolicy(3, 10);
        Assert.assertFalse(m.equals(new OldPolicies()));
        Assert.assertFalse(m.shouldFailoverToSecondary(15));
        Assert.assertTrue(m.shouldFailoverToSecondary(2));
    }
}
