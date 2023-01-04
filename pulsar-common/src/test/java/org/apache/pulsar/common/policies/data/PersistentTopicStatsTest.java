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
import static org.testng.Assert.assertFalse;

import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.testng.annotations.Test;
import org.testng.collections.Maps;
import java.util.Map;
import java.util.stream.Collectors;

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
        topicStats.addPublisher(new PublisherStatsImpl());
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
        assertEquals(topicStats.getPublishers().size(), 1);
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
        assertEquals(topicStats.getPublishers().size(), 0);
        assertEquals(topicStats.subscriptions.size(), 0);
        assertEquals(topicStats.replication.size(), 0);
        assertEquals(topicStats.offloadedStorageSize, 0);
    }

    @Test
    public void testPersistentTopicStatsAggregationPartialProducerIsNotSupported() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setMsgRateIn(1);
        publisherStats1.setSupportsPartialProducer(false);
        publisherStats1.setProducerName("name1");
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setMsgRateIn(2);
        publisherStats2.setSupportsPartialProducer(false);
        publisherStats2.setProducerName("name2");
        topicStats1.addPublisher(publisherStats1);
        topicStats1.addPublisher(publisherStats2);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        final PublisherStatsImpl publisherStats3 = new PublisherStatsImpl();
        publisherStats3.setMsgRateIn(3);
        publisherStats3.setSupportsPartialProducer(false);
        publisherStats3.setProducerName("name3");
        final PublisherStatsImpl publisherStats4 = new PublisherStatsImpl();
        publisherStats4.setMsgRateIn(4);
        publisherStats4.setSupportsPartialProducer(false);
        publisherStats4.setProducerName("name4");
        final PublisherStatsImpl publisherStats5 = new PublisherStatsImpl();
        publisherStats5.setMsgRateIn(5);
        publisherStats5.setSupportsPartialProducer(false);
        publisherStats5.setProducerName("name5");
        topicStats2.addPublisher(publisherStats3);
        topicStats2.addPublisher(publisherStats4);
        topicStats2.addPublisher(publisherStats5);
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
        assertEquals(target.getPublishers().size(), 3);
        assertEquals(target.getPublishers().get(0).getMsgRateIn(), 4);
        assertEquals(target.getPublishers().get(1).getMsgRateIn(), 6);
        assertEquals(target.getPublishers().get(2).getMsgRateIn(), 5);
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

    @Test
    public void testPersistentTopicStatsAggregationPartialProducerSupported() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setSupportsPartialProducer(true);
        publisherStats1.setProducerName("name1");
        topicStats1.addPublisher(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setSupportsPartialProducer(true);
        publisherStats2.setProducerName("name1");
        topicStats2.addPublisher(publisherStats2);
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
        assertEquals(target.getPublishers().size(), 1);
        assertEquals(target.getPublishers().get(0).getProducerName(), "name1");
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

    @Test
    public void testPersistentTopicStatsAggregationByProducerName() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.msgRateIn = 1;
        topicStats1.msgThroughputIn = 1;
        topicStats1.msgRateOut = 1;
        topicStats1.msgThroughputOut = 1;
        topicStats1.averageMsgSize = 1;
        topicStats1.storageSize = 1;
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setSupportsPartialProducer(true);
        publisherStats1.msgRateIn = 1;
        publisherStats1.setProducerName("name1");
        topicStats1.addPublisher(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.msgRateIn = 1;
        topicStats2.msgThroughputIn = 2;
        topicStats2.msgRateOut = 3;
        topicStats2.msgThroughputOut = 4;
        topicStats2.averageMsgSize = 5;
        topicStats2.storageSize = 6;
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setSupportsPartialProducer(true);
        publisherStats2.msgRateIn = 1;
        publisherStats2.setProducerName("name1");
        topicStats2.addPublisher(publisherStats2);
        topicStats2.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats2.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats3 = new TopicStatsImpl();
        topicStats3.msgRateIn = 0;
        topicStats3.msgThroughputIn = 0;
        topicStats3.msgRateOut = 0;
        topicStats3.msgThroughputOut = 0;
        topicStats3.averageMsgSize = 0;
        topicStats3.storageSize = 0;
        final PublisherStatsImpl publisherStats3 = new PublisherStatsImpl();
        publisherStats3.setSupportsPartialProducer(true);
        publisherStats3.msgRateIn = 1;
        publisherStats3.setProducerName("name2");
        topicStats3.addPublisher(publisherStats3);
        topicStats3.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats3.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);
        target.add(topicStats3);

        assertEquals(target.msgRateIn, 2.0);
        assertEquals(target.msgThroughputIn, 3.0);
        assertEquals(target.msgRateOut, 4.0);
        assertEquals(target.msgThroughputOut, 5.0);
        assertEquals(target.averageMsgSize, 2.0);
        assertEquals(target.storageSize, 7);
        assertEquals(target.getPublishers().size(), 2);
        final Map<String, Double> expectedPublishersMap = Maps.newHashMap();
        expectedPublishersMap.put("name1", 2.0);
        expectedPublishersMap.put("name2", 1.0);
        assertEquals(target.getPublishers().stream().collect(
                Collectors.toMap(PublisherStats::getProducerName, e -> ((PublisherStatsImpl) e).msgRateIn)), expectedPublishersMap);
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

    @Test
    public void testPersistentTopicStatsByNullProducerName() {
        final TopicStatsImpl topicStats1 = new TopicStatsImpl();
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setSupportsPartialProducer(false);
        publisherStats1.setProducerName(null);
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setSupportsPartialProducer(false);
        publisherStats2.setProducerName(null);
        topicStats1.addPublisher(publisherStats1);
        topicStats1.addPublisher(publisherStats2);

        assertEquals(topicStats1.getPublishers().size(), 2);
        assertFalse(topicStats1.getPublishers().get(0).isSupportsPartialProducer());
        assertFalse(topicStats1.getPublishers().get(1).isSupportsPartialProducer());

        final TopicStatsImpl topicStats2 = new TopicStatsImpl();
        final PublisherStatsImpl publisherStats3 = new PublisherStatsImpl();
        publisherStats3.setSupportsPartialProducer(true);
        publisherStats3.setProducerName(null);
        final PublisherStatsImpl publisherStats4 = new PublisherStatsImpl();
        publisherStats4.setSupportsPartialProducer(true);
        publisherStats4.setProducerName(null);
        topicStats2.addPublisher(publisherStats3);
        topicStats2.addPublisher(publisherStats4);

        assertEquals(topicStats2.getPublishers().size(), 2);
        // when the producerName is null, fall back to false
        assertFalse(topicStats2.getPublishers().get(0).isSupportsPartialProducer());
        assertFalse(topicStats2.getPublishers().get(1).isSupportsPartialProducer());

        final TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);

        assertEquals(target.getPublishers().size(), 2);
    }
}
