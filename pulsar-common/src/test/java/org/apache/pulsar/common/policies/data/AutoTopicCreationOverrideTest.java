/*
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
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.testng.annotations.Test;

public class AutoTopicCreationOverrideTest {

    @Test
    public void testValidOverrideNonPartitioned() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .build();
        assertTrue(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testValidOverridePartitioned() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(2)
                .build();
        assertTrue(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testInvalidTopicType() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType("aaa")
                .build();
        assertFalse(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testNumPartitionsTooLow() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(0)
                .build();
        assertFalse(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testNumPartitionsNotSet() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .build();
        assertFalse(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testNumPartitionsOnNonPartitionedZeroAllowed() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .defaultNumPartitions(0)
                .build();
        assertTrue(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

    @Test
    public void testNumPartitionsOnNonPartitionedOneAllowed() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .defaultNumPartitions(1)
                .build();
        assertTrue(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }


    @Test
    public void testNumPartitionsOnNonPartitionedTooHighRejected() {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.NON_PARTITIONED.toString())
                .defaultNumPartitions(2)
                .build();
        assertFalse(AutoTopicCreationOverrideImpl.validateOverride(override).isSuccess());
    }

}
