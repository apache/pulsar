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

import org.apache.pulsar.common.policies.data.stats.NonPersistentPartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentSubscriptionStatsImpl;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class NonPersistentPartitionedTopicStatsTest {

    @Test
    public void testPartitionedTopicStats() {
        NonPersistentPartitionedTopicStatsImpl nonPersistentPartitionedTopicStats = new NonPersistentPartitionedTopicStatsImpl();
        nonPersistentPartitionedTopicStats.msgRateIn = 1;
        nonPersistentPartitionedTopicStats.msgThroughputIn = 1;
        nonPersistentPartitionedTopicStats.msgRateOut = 1;
        nonPersistentPartitionedTopicStats.msgThroughputOut = 1;
        nonPersistentPartitionedTopicStats.averageMsgSize = 1;
        nonPersistentPartitionedTopicStats.storageSize = 1;
        nonPersistentPartitionedTopicStats.getPublishers().add(new NonPersistentPublisherStatsImpl());
        nonPersistentPartitionedTopicStats.getSubscriptions().put("test_ns", new NonPersistentSubscriptionStatsImpl());
        nonPersistentPartitionedTopicStats.getReplication().put("test_ns", new NonPersistentReplicatorStatsImpl());
        nonPersistentPartitionedTopicStats.metadata.partitions = 1;
        nonPersistentPartitionedTopicStats.partitions.put("test", nonPersistentPartitionedTopicStats);
        nonPersistentPartitionedTopicStats.reset();
        assertEquals(nonPersistentPartitionedTopicStats.msgRateIn, 0.0);
        assertEquals(nonPersistentPartitionedTopicStats.msgThroughputIn, 0.0);
        assertEquals(nonPersistentPartitionedTopicStats.msgRateOut, 0.0);
        assertEquals(nonPersistentPartitionedTopicStats.msgThroughputOut, 0.0);
        assertEquals(nonPersistentPartitionedTopicStats.averageMsgSize, 0.0);
        assertEquals(nonPersistentPartitionedTopicStats.storageSize, 0);
        assertEquals(nonPersistentPartitionedTopicStats.getPublishers().size(), 0);
        assertEquals(nonPersistentPartitionedTopicStats.getSubscriptions().size(), 0);
        assertEquals(nonPersistentPartitionedTopicStats.getReplication().size(), 0);
        assertEquals(nonPersistentPartitionedTopicStats.metadata.partitions, 0);
        assertEquals(nonPersistentPartitionedTopicStats.partitions.size(), 0);
    }
}
