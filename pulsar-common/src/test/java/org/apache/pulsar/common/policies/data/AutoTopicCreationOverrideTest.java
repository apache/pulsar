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

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AutoTopicCreationOverrideTest {

    @Test
    public void testValidOverrideNonPartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.NON_PARTITIONED.toString(), null);
        assertTrue(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testValidOverridePartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), 2);
        assertTrue(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testInvalidTopicType() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, "aaa", null);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsTooLow() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), 0);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsNotSet() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), null);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsOnNonPartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.NON_PARTITIONED.toString(), 2);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }
}
