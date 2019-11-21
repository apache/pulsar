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
package org.apache.pulsar.client.api;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeySharedPolicyTest {

    @Test
    public void testAutoSplit() {

        KeySharedPolicy policy = KeySharedPolicy.autoSplitHashRange();
        Assert.assertEquals(2 << 15, policy.getHashRangeTotal());

        policy.validate();
    }

    @Test
    public void testExclusiveHashRange() {

        KeySharedPolicy.KeySharedPolicySticky policy = KeySharedPolicy.stickyHashRange();
        Assert.assertEquals(2 << 15, policy.getHashRangeTotal());

        policy.ranges(Range.of(0, 1), Range.of(1, 2));
        Assert.assertEquals(policy.getRanges().size(), 2);
    }

    @Test
    public void testExclusiveHashRangeInvalid() {

        KeySharedPolicy.KeySharedPolicySticky policy = KeySharedPolicy.stickyHashRange();
        try {
            policy.validate();
            Assert.fail("should be failed");
        } catch (IllegalArgumentException ignore) {
        }

        policy.ranges(Range.of(0, 9), Range.of(0, 5));
        try {
            policy.validate();
            Assert.fail("should be failed");
        } catch (IllegalArgumentException ignore) {
        }
    }
}
