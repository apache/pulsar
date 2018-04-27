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

import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.testng.annotations.Test;

public class PersistentTopicStatsTest {

    @Test
    public void testPersistentTopicStats() {
        TopicStats topicStats = new TopicStats();
        topicStats.msgRateIn = 1;
        topicStats.msgThroughputIn = 1;
        topicStats.msgRateOut = 1;
        topicStats.msgThroughputOut = 1;
        topicStats.averageMsgSize = 1;
        topicStats.storageSize = 1;
        topicStats.publishers.add(new PublisherStats());
        topicStats.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats.replication.put("test_ns", new ReplicatorStats());
        TopicStats target = new TopicStats();
        target.add(topicStats);
        assertEquals(topicStats.msgRateIn, 1.0);
        assertEquals(topicStats.msgThroughputIn, 1.0);
        assertEquals(topicStats.msgRateOut, 1.0);
        assertEquals(topicStats.msgThroughputOut, 1.0);
        assertEquals(topicStats.averageMsgSize, 1.0);
        assertEquals(topicStats.storageSize, 1);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.subscriptions.size(), 1);
        assertEquals(topicStats.replication.size(), 1);
        topicStats.reset();
        assertEquals(topicStats.msgRateIn, 0.0);
        assertEquals(topicStats.msgThroughputIn, 0.0);
        assertEquals(topicStats.msgRateOut, 0.0);
        assertEquals(topicStats.msgThroughputOut, 0.0);
        assertEquals(topicStats.averageMsgSize, 0.0);
        assertEquals(topicStats.storageSize, 0);
        assertEquals(topicStats.publishers.size(), 0);
        assertEquals(topicStats.subscriptions.size(), 0);
        assertEquals(topicStats.replication.size(), 0);
    }

}