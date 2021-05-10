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
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;
import org.testng.collections.Maps;

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
        topicStats.offloadedStorageSize = 1;
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
        assertEquals(topicStats.offloadedStorageSize, 1);
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
        assertEquals(topicStats.offloadedStorageSize, 0);
    }

    @Test
    public void testPersistentTopicStatsAggregation() {
        TopicStats topicStats1 = new TopicStats();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        final PublisherStats publisherStats1 = new PublisherStats();
        publisherStats1.producerStatsKey = "key";
        topicStats1.publishers.add(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats1.replication.put("test_ns", new ReplicatorStats());

        TopicStats topicStats2 = new TopicStats();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        final PublisherStats publisherStats2 = new PublisherStats();
        publisherStats2.producerStatsKey = "key";
        topicStats2.publishers.add(publisherStats2);
        topicStats2.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats2.replication.put("test_ns", new ReplicatorStats());

        TopicStats target = new TopicStats();
        target.add(topicStats1);
        target.add(topicStats2);

        assertEquals(target.msgRateIn, 2.0);
        assertEquals(target.msgThroughputIn, 3.0);
        assertEquals(target.msgRateOut, 4.0);
        assertEquals(target.msgThroughputOut, 5.0);
        assertEquals(target.averageMsgSize, 3.0);
        assertEquals(target.storageSize, 7);
        assertEquals(target.publishers.size(), 1);
        assertEquals(target.publishers.get(0).producerStatsKey, "key");
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

    @Test
    public void testPersistentTopicStatsAggregationDifferentKeys() {
        TopicStats topicStats1 = new TopicStats();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        final PublisherStats publisherStats1 = new PublisherStats();
        publisherStats1.msgRateIn = 1;
        publisherStats1.producerStatsKey = "key1";
        topicStats1.publishers.add(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats1.replication.put("test_ns", new ReplicatorStats());

        TopicStats topicStats2 = new TopicStats();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        final PublisherStats publisherStats2 = new PublisherStats();
        publisherStats2.msgRateIn = 1;
        publisherStats2.producerStatsKey = "key1";
        topicStats2.publishers.add(publisherStats2);
        topicStats2.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats2.replication.put("test_ns", new ReplicatorStats());

        TopicStats topicStats3 = new TopicStats();
        topicStats3.msgRateIn = 0;
        topicStats3.msgThroughputIn = 0;
        topicStats3.msgRateOut = 0;
        topicStats3.msgThroughputOut = 0;
        topicStats3.averageMsgSize = 0;
        topicStats3.storageSize = 0;
        final PublisherStats publisherStats3 = new PublisherStats();
        publisherStats3.msgRateIn = 1;
        publisherStats3.producerStatsKey = "key2";
        topicStats3.publishers.add(publisherStats3);
        topicStats3.subscriptions.put("test_ns", new SubscriptionStats());
        topicStats3.replication.put("test_ns", new ReplicatorStats());

        TopicStats target = new TopicStats();
        target.add(topicStats1);
        target.add(topicStats2);
        target.add(topicStats3);

        assertEquals(target.msgRateIn, 2.0);
        assertEquals(target.msgThroughputIn, 3.0);
        assertEquals(target.msgRateOut, 4.0);
        assertEquals(target.msgThroughputOut, 5.0);
        assertEquals(target.averageMsgSize, 2.0);
        assertEquals(target.storageSize, 7);
        assertEquals(target.publishers.size(), 2);
        final Map<String, Double> expectedPublishersMap = Maps.newHashMap();
        expectedPublishersMap.put("key1", 2.0);
        expectedPublishersMap.put("key2", 1.0);
        assertEquals(target.publishers.stream().collect(
                Collectors.toMap(e -> e.producerStatsKey, e -> e.msgRateIn)), expectedPublishersMap);
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

}
