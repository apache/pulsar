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

import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.testng.annotations.Test;

public class PersistentTopicStatsTest {

    @Test
    public void testPersistentTopicStats() {
        TopicStatsImpl topicStats = new TopicStatsImpl();
        topicStats.msgRateIn = 1;
        topicStats.msgThroughputIn = 1;
        topicStats.msgRateOut = 1;
        topicStats.msgThroughputOut = 1;
        topicStats.averageMsgSize = 1;
        topicStats.storageSize = 1;
        topicStats.offloadedStorageSize = 1;
        topicStats.publishers.add(new PublisherStatsImpl());
        topicStats.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats.replication.put("test_ns", new ReplicatorStatsImpl());
        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats);
        assertEquals(topicStats.msgRateIn, 1.0);
        assertEquals(topicStats.msgThroughputIn, 1.0);
        assertEquals(topicStats.msgRateOut, 1.0);
        assertEquals(topicStats.msgThroughputOut, 1.0);
        assertEquals(topicStats.averageMsgSize, 1.0);
        assertEquals(topicStats.offloadedStorageSize, 1);
        assertEquals(topicStats.lastOffloadLedgerId, 0);
        assertEquals(topicStats.lastOffloadSuccessTimeStamp, 0);
        assertEquals(topicStats.lastOffloadFailureTimeStamp, 0);
        assertEquals(topicStats.storageSize, 1);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.subscriptions.size(), 1);
        assertEquals(topicStats.replication.size(), 1);
        assertEquals(topicStats.compaction.lastCompactionRemovedEventCount, 0);
        assertEquals(topicStats.compaction.lastCompactionSucceedTimestamp, 0);
        assertEquals(topicStats.compaction.lastCompactionFailedTimestamp, 0);
        assertEquals(topicStats.compaction.lastCompactionDurationTimeInMills, 0);
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
        assertEquals(topicStats.offloadedStorageSize, 0);
    }

    @Test
    public void testPersistentTopicStatsAggregation() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        topicStats1.publishers.add(new PublisherStatsImpl());
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        topicStats2.publishers.add(new PublisherStatsImpl());
        topicStats2.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats2.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);

        assertEquals(target.msgRateIn, 2.0);
        assertEquals(target.msgThroughputIn, 3.0);
        assertEquals(target.msgRateOut, 4.0);
        assertEquals(target.msgThroughputOut, 5.0);
        assertEquals(target.averageMsgSize, 3.0);
        assertEquals(target.storageSize, 7);
        assertEquals(target.publishers.size(), 1);
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

}
