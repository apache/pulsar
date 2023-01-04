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
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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

    @Test
    public void testPartitionedTopicStatsByNullProducerName() {
        final NonPersistentTopicStatsImpl topicStats1 = new NonPersistentTopicStatsImpl();
        final NonPersistentPublisherStatsImpl publisherStats1 = new NonPersistentPublisherStatsImpl();
        publisherStats1.setMsgRateIn(1);
        publisherStats1.setSupportsPartialProducer(false);
        publisherStats1.setProducerName(null);
        final NonPersistentPublisherStatsImpl publisherStats2 = new NonPersistentPublisherStatsImpl();
        publisherStats2.setMsgRateIn(2);
        publisherStats2.setSupportsPartialProducer(false);
        publisherStats2.setProducerName(null);
        topicStats1.addPublisher(publisherStats1);
        topicStats1.addPublisher(publisherStats2);

        assertEquals(topicStats1.getPublishers().size(), 2);
        assertFalse(topicStats1.getPublishers().get(0).isSupportsPartialProducer());
        assertFalse(topicStats1.getPublishers().get(1).isSupportsPartialProducer());

        final NonPersistentTopicStatsImpl topicStats2 = new NonPersistentTopicStatsImpl();
        final NonPersistentPublisherStatsImpl publisherStats3 = new NonPersistentPublisherStatsImpl();
        publisherStats3.setMsgRateIn(3);
        publisherStats3.setSupportsPartialProducer(true);
        publisherStats3.setProducerName(null);
        final NonPersistentPublisherStatsImpl publisherStats4 = new NonPersistentPublisherStatsImpl();
        publisherStats4.setMsgRateIn(4);
        publisherStats4.setSupportsPartialProducer(true);
        publisherStats4.setProducerName(null);
        final NonPersistentPublisherStatsImpl publisherStats5 = new NonPersistentPublisherStatsImpl();
        publisherStats5.setMsgRateIn(5);
        publisherStats5.setSupportsPartialProducer(true);
        publisherStats5.setProducerName(null);
        topicStats2.addPublisher(publisherStats3);
        topicStats2.addPublisher(publisherStats4);
        topicStats2.addPublisher(publisherStats5);

        assertEquals(topicStats2.getPublishers().size(), 3);
        // when the producerName is null, fall back to false
        assertFalse(topicStats2.getPublishers().get(0).isSupportsPartialProducer());
        assertFalse(topicStats2.getPublishers().get(1).isSupportsPartialProducer());

        final NonPersistentPartitionedTopicStatsImpl target = new NonPersistentPartitionedTopicStatsImpl();
        target.add(topicStats1);
        target.add(topicStats2);

        assertEquals(target.getPublishers().size(), 3);
        assertEquals(target.getPublishers().get(0).getMsgRateIn(), 4);
        assertEquals(target.getPublishers().get(1).getMsgRateIn(), 6);
        assertEquals(target.getPublishers().get(2).getMsgRateIn(), 5);
    }
}
