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
        topicStats.setMsgRateIn(1);
        topicStats.setMsgThroughputIn(1);
        topicStats.setMsgRateOut(1);
        topicStats.setMsgThroughputOut(1);
        topicStats.setAverageMsgSize(1);
        topicStats.setStorageSize(1);
        topicStats.setOffloadedStorageSize(1);
        topicStats.addPublisher(new PublisherStatsImpl());
        topicStats.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats.replication.put("test_ns", new ReplicatorStatsImpl());
        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats);
        assertEquals(topicStats.getMsgRateIn(), 1.0);
        assertEquals(topicStats.getMsgThroughputIn(), 1.0);
        assertEquals(topicStats.getMsgRateOut(), 1.0);
        assertEquals(topicStats.getMsgThroughputOut(), 1.0);
        assertEquals(topicStats.getAverageMsgSize(), 1.0);
        assertEquals(topicStats.getOffloadedStorageSize(), 1);
        assertEquals(topicStats.getLastOffloadLedgerId(), 0);
        assertEquals(topicStats.getLastOffloadSuccessTimeStamp(), 0);
        assertEquals(topicStats.getLastOffloadFailureTimeStamp(), 0);
        assertEquals(topicStats.getStorageSize(), 1);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.subscriptions.size(), 1);
        assertEquals(topicStats.replication.size(), 1);
        assertEquals(topicStats.getCompaction().getLastCompactionRemovedEventCount(), 0);
        assertEquals(topicStats.getCompaction().getLastCompactionSucceedTimestamp(), 0);
        assertEquals(topicStats.getCompaction().getLastCompactionFailedTimestamp(), 0);
        assertEquals(topicStats.getCompaction().getLastCompactionDurationTimeInMills(), 0);
        topicStats.reset();
        assertEquals(topicStats.getMsgRateIn(), 0.0);
        assertEquals(topicStats.getMsgThroughputIn(), 0.0);
        assertEquals(topicStats.getMsgRateOut(), 0.0);
        assertEquals(topicStats.getMsgThroughputOut(), 0.0);
        assertEquals(topicStats.getAverageMsgSize(), 0.0);
        assertEquals(topicStats.getStorageSize(), 0);
        assertEquals(topicStats.getPublishers().size(), 0);
        assertEquals(topicStats.subscriptions.size(), 0);
        assertEquals(topicStats.replication.size(), 0);
        assertEquals(topicStats.getOffloadedStorageSize(), 0);
    }

    @Test
    public void testPersistentTopicStatsAggregationPartialProducerIsNotSupported() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.setMsgRateIn(1);
        topicStats1.setMsgThroughputIn(1);
        topicStats1.setMsgRateOut(1);
        topicStats1.setMsgThroughputOut(1);
        topicStats1.setAverageMsgSize(1);
        topicStats1.setStorageSize(1);
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
        topicStats2.setMsgRateIn(1);
        topicStats2.setMsgThroughputIn(2);
        topicStats2.setMsgRateOut(3);
        topicStats2.setMsgThroughputOut(4);
        topicStats2.setAverageMsgSize(5);
        topicStats2.setStorageSize(6);
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

        assertEquals(target.getMsgRateIn(), 2.0);
        assertEquals(target.getMsgThroughputIn(), 3.0);
        assertEquals(target.getMsgRateOut(), 4.0);
        assertEquals(target.getMsgThroughputOut(), 5.0);
        assertEquals(target.getAverageMsgSize(), 3.0);
        assertEquals(target.getStorageSize(), 7);
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
        topicStats1.setMsgRateIn(1);
        topicStats1.setMsgThroughputIn(1);
        topicStats1.setMsgRateOut(1);
        topicStats1.setMsgThroughputOut(1);
        topicStats1.setAverageMsgSize(1);
        topicStats1.setStorageSize(1);
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setSupportsPartialProducer(true);
        publisherStats1.setProducerName("name1");
        topicStats1.addPublisher(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.setMsgRateIn(1);
        topicStats2.setMsgThroughputIn(2);
        topicStats2.setMsgRateOut(3);
        topicStats2.setMsgThroughputOut(4);
        topicStats2.setAverageMsgSize(5);
        topicStats2.setStorageSize(6);
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setSupportsPartialProducer(true);
        publisherStats2.setProducerName("name1");
        topicStats2.addPublisher(publisherStats2);
        topicStats2.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats2.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);

        assertEquals(target.getMsgRateIn(), 2.0);
        assertEquals(target.getMsgThroughputIn(), 3.0);
        assertEquals(target.getMsgRateOut(), 4.0);
        assertEquals(target.getMsgThroughputOut(), 5.0);
        assertEquals(target.getAverageMsgSize(), 3.0);
        assertEquals(target.getStorageSize(), 7);
        assertEquals(target.getPublishers().size(), 1);
        assertEquals(target.getPublishers().get(0).getProducerName(), "name1");
        assertEquals(target.subscriptions.size(), 1);
        assertEquals(target.replication.size(), 1);
    }

    @Test
    public void testPersistentTopicStatsAggregationByProducerName() {
        TopicStatsImpl topicStats1 = new TopicStatsImpl();
        topicStats1.setMsgRateIn(1);
        topicStats1.setMsgThroughputIn(1);
        topicStats1.setMsgRateOut(1);
        topicStats1.setMsgThroughputOut(1);
        topicStats1.setAverageMsgSize(1);
        topicStats1.setStorageSize(1);
        final PublisherStatsImpl publisherStats1 = new PublisherStatsImpl();
        publisherStats1.setSupportsPartialProducer(true);
        publisherStats1.setMsgRateIn(1);
        publisherStats1.setProducerName("name1");
        topicStats1.addPublisher(publisherStats1);
        topicStats1.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats1.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats2 = new TopicStatsImpl();
        topicStats2.setMsgRateIn(1);
        topicStats2.setMsgThroughputIn(2);
        topicStats2.setMsgRateOut(3);
        topicStats2.setMsgThroughputOut(4);
        topicStats2.setAverageMsgSize(5);
        topicStats2.setStorageSize(6);
        final PublisherStatsImpl publisherStats2 = new PublisherStatsImpl();
        publisherStats2.setSupportsPartialProducer(true);
        publisherStats2.setMsgRateIn(1);
        publisherStats2.setProducerName("name1");
        topicStats2.addPublisher(publisherStats2);
        topicStats2.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats2.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl topicStats3 = new TopicStatsImpl();
        topicStats3.setMsgRateIn(0);
        topicStats3.setMsgThroughputIn(0);
        topicStats3.setMsgRateOut(0);
        topicStats3.setMsgThroughputOut(0);
        topicStats3.setAverageMsgSize(0);
        topicStats3.setStorageSize(0);
        final PublisherStatsImpl publisherStats3 = new PublisherStatsImpl();
        publisherStats3.setSupportsPartialProducer(true);
        publisherStats3.setMsgRateIn(1);
        publisherStats3.setProducerName("name2");
        topicStats3.addPublisher(publisherStats3);
        topicStats3.subscriptions.put("test_ns", new SubscriptionStatsImpl());
        topicStats3.replication.put("test_ns", new ReplicatorStatsImpl());

        TopicStatsImpl target = new TopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);
        target.add(topicStats3);

        assertEquals(target.getMsgRateIn(), 2.0);
        assertEquals(target.getMsgThroughputIn(), 3.0);
        assertEquals(target.getMsgRateOut(), 4.0);
        assertEquals(target.getMsgThroughputOut(), 5.0);
        assertEquals(target.getAverageMsgSize(), 2.0);
        assertEquals(target.getStorageSize(), 7);
        assertEquals(target.getPublishers().size(), 2);
        final Map<String, Double> expectedPublishersMap = Maps.newHashMap();
        expectedPublishersMap.put("name1", 2.0);
        expectedPublishersMap.put("name2", 1.0);
        assertEquals(target.getPublishers().stream().collect(
                Collectors.toMap(PublisherStats::getProducerName, e -> ((PublisherStatsImpl) e).getMsgRateIn())), expectedPublishersMap);
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
