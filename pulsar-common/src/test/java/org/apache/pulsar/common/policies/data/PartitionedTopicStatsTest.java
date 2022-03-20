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

import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.testng.annotations.Test;

public class PartitionedTopicStatsTest {

    @Test
    public void testPartitionedTopicStats() {
        PartitionedTopicStatsImpl partitionedTopicStats = new PartitionedTopicStatsImpl();
        partitionedTopicStats.msgRateIn = 1;
        partitionedTopicStats.msgThroughputIn = 1;
        partitionedTopicStats.msgRateOut = 1;
        partitionedTopicStats.msgThroughputOut = 1;
        partitionedTopicStats.averageMsgSize = 1;
        partitionedTopicStats.storageSize = 1;
        partitionedTopicStats.addPublisher((new PublisherStatsImpl()));
        partitionedTopicStats.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        partitionedTopicStats.replication.put("test_ns", new ReplicatorStatsImpl());
        partitionedTopicStats.metadata.partitions = 1;
        partitionedTopicStats.partitions.put("test", partitionedTopicStats);
        partitionedTopicStats.reset();
        assertEquals(partitionedTopicStats.msgRateIn, 0.0);
        assertEquals(partitionedTopicStats.msgThroughputIn, 0.0);
        assertEquals(partitionedTopicStats.msgRateOut, 0.0);
        assertEquals(partitionedTopicStats.msgThroughputOut, 0.0);
        assertEquals(partitionedTopicStats.averageMsgSize, 0.0);
        assertEquals(partitionedTopicStats.storageSize, 0);
        assertEquals(partitionedTopicStats.getPublishers().size(), 0);
        assertEquals(partitionedTopicStats.subscriptions.size(), 0);
        assertEquals(partitionedTopicStats.replication.size(), 0);
        assertEquals(partitionedTopicStats.metadata.partitions, 0);
        assertEquals(partitionedTopicStats.partitions.size(), 0);
    }
}