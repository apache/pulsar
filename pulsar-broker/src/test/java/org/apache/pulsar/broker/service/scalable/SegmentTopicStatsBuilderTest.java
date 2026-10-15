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
package org.apache.pulsar.broker.service.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Set;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.DateFormatter;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link SegmentTopicStatsBuilder}: the regular topic stats of a segment are
 * trimmed to {@link SegmentTopicStats}, with rates rounded and times rendered as dates.
 */
public class SegmentTopicStatsBuilderTest {

    private static final long CREATED_AT = 1789680081733L;
    private static final long LAST_ACTIVITY = 1789682261518L;

    private static TopicStatsImpl topicStats() {
        TopicStatsImpl ts = new TopicStatsImpl();
        ts.ownerBroker = "localhost:8080";
        ts.msgRateIn = 100.78959223736648;
        ts.msgThroughputIn = 5462.888878593895;
        ts.msgRateOut = 619.2271279977319;
        ts.msgThroughputOut = 33562.681580215074;
        ts.averageMsgSize = 54.200922509225094;
        ts.storageSize = 379459;
        ts.backlogSize = 2929;
        ts.offloadedStorageSize = 7;
        ts.oldestBacklogMessageAgeSeconds = -1;
        ts.waitingPublishers = 2;
        ts.topicCreationTimeStamp = CREATED_AT;
        // Fields the trimmed view must not carry over.
        ts.msgInCounter = 6978;
        ts.bytesInCounter = 379459;
        ts.backlogQuotaLimitSize = 10737418240L;
        ts.lastPublishTimeStamp = LAST_ACTIVITY;

        PublisherStatsImpl publisher = new PublisherStatsImpl();
        publisher.setProducerName("standalone-60-0");
        publisher.accessMode = ProducerAccessMode.Shared;
        publisher.msgRateIn = 100.78959223736648;
        publisher.msgThroughputIn = 5462.888878593895;
        publisher.averageMsgSize = 54.200922509225094;
        publisher.setAddress("/127.0.0.1:56657");
        publisher.setConnectedSince("2026-09-17T14:56:32.182621-07:00");
        publisher.setClientVersion("Pulsar-Java-v5.0.0-SNAPSHOT");
        ts.addPublisher(publisher);

        ConsumerStatsImpl consumer = new ConsumerStatsImpl();
        consumer.consumerName = "v5-stream-ku0LATMI-seg-24";
        consumer.msgRateOut = 619.2271279977319;
        consumer.msgThroughputOut = 33562.681580215074;
        consumer.messageAckRate = 618.0844630177503;
        consumer.availablePermits = 523;
        consumer.unackedMessages = 2;
        consumer.blockedConsumerOnUnackedMsgs = true;
        consumer.setAddress("/127.0.0.1:56712");
        consumer.setConnectedSince("2026-09-17T14:57:17.205034-07:00");
        consumer.setClientVersion("Pulsar-Java-v5.0.0-SNAPSHOT");
        consumer.lastAckedTimestamp = LAST_ACTIVITY;
        consumer.lastConsumedTimestamp = 0;

        SubscriptionStatsImpl sub = new SubscriptionStatsImpl();
        sub.type = "Key_Shared";
        sub.msgRateOut = 619.2271279977319;
        sub.msgThroughputOut = 33562.681580215074;
        sub.messageAckRate = 618.0844630177503;
        sub.msgBacklog = 3;
        sub.backlogSize = 2929;
        sub.unackedMessages = 2;
        sub.lastConsumedTimestamp = LAST_ACTIVITY;
        sub.lastAckedTimestamp = 0;
        sub.consumers.add(consumer);
        ts.subscriptions.put("sub", sub);
        return ts;
    }

    @Test
    public void testTopicLevelFields() {
        SegmentTopicStats stats = SegmentTopicStatsBuilder.fromTopicStats(topicStats());

        assertEquals(stats.getOwnerBroker(), "localhost:8080");
        assertEquals(stats.getMsgRateIn(), 100.79);
        assertEquals(stats.getByteRateIn(), 5462.889);
        assertEquals(stats.getMsgRateOut(), 619.227);
        assertEquals(stats.getByteRateOut(), 33562.682);
        assertEquals(stats.getAverageMsgSize(), 54.201);
        assertEquals(stats.getStorageSize(), 379459L);
        assertEquals(stats.getBacklogSize(), 2929L);
        assertEquals(stats.getOffloadedStorageSize(), 7L);
        assertEquals(stats.getOldestBacklogMessageAgeSeconds(), -1L);
        assertEquals(stats.getWaitingPublishers(), 2);
        assertEquals(stats.getTopicCreationTime(), DateFormatter.format(CREATED_AT));
    }

    @Test
    public void testProducers() {
        SegmentTopicStats stats = SegmentTopicStatsBuilder.fromTopicStats(topicStats());

        assertEquals(stats.getProducers().size(), 1);
        SegmentTopicStats.ProducerStats producer = stats.getProducers().get(0);
        assertEquals(producer.getProducerName(), "standalone-60-0");
        assertEquals(producer.getAccessMode(), ProducerAccessMode.Shared);
        assertEquals(producer.getMsgRateIn(), 100.79);
        assertEquals(producer.getByteRateIn(), 5462.889);
        assertEquals(producer.getAverageMsgSize(), 54.201);
        assertEquals(producer.getAddress(), "/127.0.0.1:56657");
        assertEquals(producer.getConnectedSince(), "2026-09-17T14:56:32.182621-07:00");
        assertEquals(producer.getClientVersion(), "Pulsar-Java-v5.0.0-SNAPSHOT");
    }

    @Test
    public void testSubscriptionsAndConsumers() {
        SegmentTopicStats stats = SegmentTopicStatsBuilder.fromTopicStats(topicStats());

        assertEquals(stats.getSubscriptions().keySet(), Set.of("sub"));
        SegmentTopicStats.SubscriptionStats sub = stats.getSubscriptions().get("sub");
        assertEquals(sub.getType(), "Key_Shared");
        assertEquals(sub.getMsgRateOut(), 619.227);
        assertEquals(sub.getByteRateOut(), 33562.682);
        assertEquals(sub.getMessageAckRate(), 618.084);
        assertEquals(sub.getMsgBacklog(), 3L);
        assertEquals(sub.getBacklogSize(), 2929L);
        assertEquals(sub.getUnackedMessages(), 2L);
        assertEquals(sub.getLastConsumedTime(), DateFormatter.format(LAST_ACTIVITY));
        assertNull(sub.getLastAckedTime(), "a zero timestamp means never, so the date is absent");

        assertEquals(sub.getConsumers().size(), 1);
        SegmentTopicStats.ConsumerStats consumer = sub.getConsumers().get(0);
        assertEquals(consumer.getConsumerName(), "v5-stream-ku0LATMI-seg-24");
        assertEquals(consumer.getMsgRateOut(), 619.227);
        assertEquals(consumer.getByteRateOut(), 33562.682);
        assertEquals(consumer.getMessageAckRate(), 618.084);
        assertEquals(consumer.getAvailablePermits(), 523);
        assertEquals(consumer.getUnackedMessages(), 2L);
        assertTrue(consumer.isBlockedConsumerOnUnackedMsgs());
        assertEquals(consumer.getAddress(), "/127.0.0.1:56712");
        assertEquals(consumer.getClientVersion(), "Pulsar-Java-v5.0.0-SNAPSHOT");
        assertEquals(consumer.getLastAckedTime(), DateFormatter.format(LAST_ACTIVITY));
        assertNull(consumer.getLastConsumedTime());
    }

    @Test
    public void testUnknownCreationTimeIsAbsent() {
        TopicStatsImpl ts = new TopicStatsImpl();
        SegmentTopicStats stats = SegmentTopicStatsBuilder.fromTopicStats(ts);
        assertNull(stats.getTopicCreationTime());
        assertTrue(stats.getProducers().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }
}
