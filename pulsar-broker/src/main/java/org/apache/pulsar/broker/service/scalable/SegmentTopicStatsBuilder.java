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

import static org.apache.pulsar.broker.service.scalable.ScalableTopicStatsBuilder.round;
import java.util.Map;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.DateFormatter;

/**
 * Trims the regular {@link TopicStats} of a segment's backing topic down to a
 * {@link SegmentTopicStats}: rates rounded to three decimals and renamed to
 * {@code byteRate*}, epoch timestamps rendered as ISO-8601 dates, and the lifetime counters,
 * quota, offload, compaction, transaction, Key_Shared and throttling internals left out.
 */
public final class SegmentTopicStatsBuilder {

    private SegmentTopicStatsBuilder() {
    }

    public static SegmentTopicStats fromTopicStats(TopicStats ts) {
        SegmentTopicStats stats = new SegmentTopicStats();
        stats.setOwnerBroker(ts.getOwnerBroker());
        stats.setMsgRateIn(round(ts.getMsgRateIn()));
        stats.setByteRateIn(round(ts.getMsgThroughputIn()));
        stats.setMsgRateOut(round(ts.getMsgRateOut()));
        stats.setByteRateOut(round(ts.getMsgThroughputOut()));
        stats.setAverageMsgSize(round(ts.getAverageMsgSize()));
        stats.setStorageSize(ts.getStorageSize());
        stats.setBacklogSize(ts.getBacklogSize());
        stats.setOffloadedStorageSize(ts.getOffloadedStorageSize());
        stats.setOldestBacklogMessageAgeSeconds(ts.getOldestBacklogMessageAgeSeconds());
        stats.setWaitingPublishers(ts.getWaitingPublishers());
        stats.setTopicCreationTime(date(ts.getTopicCreationTimeStamp()));

        for (PublisherStats publisher : ts.getPublishers()) {
            SegmentTopicStats.ProducerStats producer = new SegmentTopicStats.ProducerStats();
            producer.setProducerName(publisher.getProducerName());
            producer.setAccessMode(publisher.getAccessMode());
            producer.setMsgRateIn(round(publisher.getMsgRateIn()));
            producer.setByteRateIn(round(publisher.getMsgThroughputIn()));
            producer.setAverageMsgSize(round(publisher.getAverageMsgSize()));
            producer.setAddress(publisher.getAddress());
            producer.setConnectedSince(publisher.getConnectedSince());
            producer.setClientVersion(publisher.getClientVersion());
            stats.getProducers().add(producer);
        }

        for (Map.Entry<String, ? extends SubscriptionStats> entry : ts.getSubscriptions().entrySet()) {
            stats.getSubscriptions().put(entry.getKey(), subscription(entry.getValue()));
        }
        return stats;
    }

    private static SegmentTopicStats.SubscriptionStats subscription(SubscriptionStats ss) {
        SegmentTopicStats.SubscriptionStats sub = new SegmentTopicStats.SubscriptionStats();
        sub.setType(ss.getType());
        sub.setMsgRateOut(round(ss.getMsgRateOut()));
        sub.setByteRateOut(round(ss.getMsgThroughputOut()));
        sub.setMsgRateRedeliver(round(ss.getMsgRateRedeliver()));
        sub.setMessageAckRate(round(ss.getMessageAckRate()));
        sub.setMsgBacklog(ss.getMsgBacklog());
        sub.setBacklogSize(ss.getBacklogSize());
        sub.setUnackedMessages(ss.getUnackedMessages());
        sub.setOldestBacklogMessageAgeSeconds(ss.getOldestBacklogMessageAgeSeconds());
        sub.setBlockedSubscriptionOnUnackedMsgs(ss.isBlockedSubscriptionOnUnackedMsgs());
        sub.setLastConsumedTime(date(ss.getLastConsumedTimestamp()));
        sub.setLastAckedTime(date(ss.getLastAckedTimestamp()));
        for (ConsumerStats consumer : ss.getConsumers()) {
            sub.getConsumers().add(consumer(consumer));
        }
        return sub;
    }

    private static SegmentTopicStats.ConsumerStats consumer(ConsumerStats cs) {
        SegmentTopicStats.ConsumerStats consumer = new SegmentTopicStats.ConsumerStats();
        consumer.setConsumerName(cs.getConsumerName());
        consumer.setMsgRateOut(round(cs.getMsgRateOut()));
        consumer.setByteRateOut(round(cs.getMsgThroughputOut()));
        consumer.setMsgRateRedeliver(round(cs.getMsgRateRedeliver()));
        consumer.setMessageAckRate(round(cs.getMessageAckRate()));
        consumer.setAvailablePermits(cs.getAvailablePermits());
        consumer.setUnackedMessages(cs.getUnackedMessages());
        consumer.setBlockedConsumerOnUnackedMsgs(cs.isBlockedConsumerOnUnackedMsgs());
        consumer.setAddress(cs.getAddress());
        consumer.setConnectedSince(cs.getConnectedSince());
        consumer.setClientVersion(cs.getClientVersion());
        consumer.setLastConsumedTime(date(cs.getLastConsumedTimestamp()));
        consumer.setLastAckedTime(date(cs.getLastAckedTimestamp()));
        return consumer;
    }

    /** An epoch-millis timestamp as an ISO-8601 date, or {@code null} when it was never set. */
    private static String date(long epochMillis) {
        return epochMillis > 0 ? DateFormatter.format(epochMillis) : null;
    }
}
