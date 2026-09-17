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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;

/**
 * Assembles a {@link ScalableTopicStats} snapshot out of the segment DAG, the per-segment
 * {@link TopicStats} collected from the segment-owning brokers, the persisted subscription
 * types and the controller's STREAM consumer sessions.
 *
 * <p>Pure aggregation — no I/O — so the folding rules can be unit-tested on their own:
 * <ul>
 *   <li>Topic-level rates, counters, storage and backlog are the sums over every segment
 *       whose stats were collected; a segment without stats keeps its DAG entry with no
 *       owner and zero load.</li>
 *   <li>A V5 producer or consumer opens one underlying producer/consumer per segment, named
 *       {@code <name>-seg-<segmentId>}. Those are folded back into a single entry keyed by
 *       {@code <name>}; an underlying producer/consumer that doesn't follow the pattern (no
 *       name was configured, so the broker assigned one per segment) is listed on its own.</li>
 *   <li>On a STREAM subscription the controller's sessions seed the consumer list, so a
 *       registered consumer is visible while disconnected within its grace period or while
 *       idle, together with the segments assigned to it.</li>
 * </ul>
 */
public final class ScalableTopicStatsBuilder {

    /** Separator the V5 SDK puts between a producer/consumer name and the segment ID. */
    private static final String SEGMENT_SUFFIX = "-seg-";

    /**
     * A consumer session held by the controller for a STREAM subscription.
     *
     * @param consumerName       stable session identity
     * @param connected          whether the consumer is currently connected
     * @param assignedSegmentIds segments the controller has assigned to the consumer
     */
    public record StreamConsumer(String consumerName, boolean connected, List<Long> assignedSegmentIds) {
    }

    private ScalableTopicStatsBuilder() {
    }

    /**
     * Build the snapshot.
     *
     * @param topic           the scalable topic
     * @param layout          the current segment DAG
     * @param segmentStats    per-segment topic stats keyed by segment ID; segments whose stats
     *                        could not be collected are absent
     * @param persistedTypes  subscription types recorded in the metadata store, keyed by name
     * @param streamConsumers the controller's consumer sessions per STREAM subscription
     */
    public static ScalableTopicStats build(TopicName topic,
                                           SegmentLayout layout,
                                           Map<Long, TopicStats> segmentStats,
                                           Map<String, ScalableSubscriptionType> persistedTypes,
                                           Map<String, List<StreamConsumer>> streamConsumers) {
        ScalableTopicStats stats = new ScalableTopicStats();
        stats.setEpoch(layout.getEpoch());
        addSegments(topic, layout, segmentStats, stats);
        addProducers(layout, segmentStats, stats);
        addSubscriptions(layout, segmentStats, persistedTypes, streamConsumers, stats);
        return stats;
    }

    private static void addSegments(TopicName topic, SegmentLayout layout,
                                    Map<Long, TopicStats> segmentStats, ScalableTopicStats stats) {
        int active = 0;
        int sealed = 0;
        for (SegmentInfo segment : layout.getAllSegments().values()) {
            if (segment.isActive()) {
                active++;
            } else {
                sealed++;
            }
            ScalableTopicStats.SegmentStats node = new ScalableTopicStats.SegmentStats();
            node.setSegmentId(segment.segmentId());
            node.setTopic(SegmentTopicName.backingTopicName(topic, segment));
            node.setHashRange(new ScalableTopicMetadata.HashRange(
                    segment.hashRange().start(), segment.hashRange().end()));
            node.setState(segment.state().name());
            node.setParentIds(new ArrayList<>(segment.parentIds()));
            node.setChildIds(new ArrayList<>(segment.childIds()));
            node.setCreatedAtMs(segment.createdAtMs());
            node.setSealedAtMs(segment.sealedAtMs());
            node.setEntryBuckets(segment.bucketCount());

            TopicStats ts = segmentStats.get(segment.segmentId());
            if (ts != null) {
                node.setOwnerBroker(ts.getOwnerBroker());
                node.setMsgRateIn(ts.getMsgRateIn());
                node.setMsgThroughputIn(ts.getMsgThroughputIn());
                node.setMsgRateOut(ts.getMsgRateOut());
                node.setMsgThroughputOut(ts.getMsgThroughputOut());
                node.setStorageSize(ts.getStorageSize());

                stats.setMsgRateIn(stats.getMsgRateIn() + ts.getMsgRateIn());
                stats.setMsgThroughputIn(stats.getMsgThroughputIn() + ts.getMsgThroughputIn());
                stats.setMsgRateOut(stats.getMsgRateOut() + ts.getMsgRateOut());
                stats.setMsgThroughputOut(stats.getMsgThroughputOut() + ts.getMsgThroughputOut());
                stats.setMsgInCounter(stats.getMsgInCounter() + ts.getMsgInCounter());
                stats.setBytesInCounter(stats.getBytesInCounter() + ts.getBytesInCounter());
                stats.setMsgOutCounter(stats.getMsgOutCounter() + ts.getMsgOutCounter());
                stats.setBytesOutCounter(stats.getBytesOutCounter() + ts.getBytesOutCounter());
                stats.setStorageSize(stats.getStorageSize() + ts.getStorageSize());
                stats.setBacklogSize(stats.getBacklogSize() + ts.getBacklogSize());
            }
            stats.getSegments().put(segment.segmentId(), node);
        }
        stats.setTotalSegments(active + sealed);
        stats.setActiveSegments(active);
        stats.setSealedSegments(sealed);
        stats.setAverageMsgSize(averageSize(stats.getMsgRateIn(), stats.getMsgThroughputIn()));
    }

    private static void addProducers(SegmentLayout layout, Map<Long, TopicStats> segmentStats,
                                     ScalableTopicStats stats) {
        Map<String, ScalableTopicStats.ProducerStats> byName = new LinkedHashMap<>();
        for (Long segmentId : layout.getAllSegments().keySet()) {
            TopicStats ts = segmentStats.get(segmentId);
            if (ts == null) {
                continue;
            }
            for (PublisherStats publisher : ts.getPublishers()) {
                String name = stripSegmentSuffix(publisher.getProducerName(), segmentId);
                ScalableTopicStats.ProducerStats producer = byName.computeIfAbsent(name, n -> {
                    ScalableTopicStats.ProducerStats p = new ScalableTopicStats.ProducerStats();
                    p.setProducerName(n);
                    p.setAccessMode(publisher.getAccessMode());
                    p.setAddress(publisher.getAddress());
                    p.setConnectedSince(publisher.getConnectedSince());
                    p.setClientVersion(publisher.getClientVersion());
                    return p;
                });
                producer.getSegmentIds().add(segmentId);
                producer.setMsgRateIn(producer.getMsgRateIn() + publisher.getMsgRateIn());
                producer.setMsgThroughputIn(producer.getMsgThroughputIn() + publisher.getMsgThroughputIn());
            }
        }
        for (ScalableTopicStats.ProducerStats producer : byName.values()) {
            producer.setAverageMsgSize(averageSize(producer.getMsgRateIn(), producer.getMsgThroughputIn()));
        }
        stats.setProducers(new ArrayList<>(byName.values()));
    }

    private static void addSubscriptions(SegmentLayout layout, Map<Long, TopicStats> segmentStats,
                                         Map<String, ScalableSubscriptionType> persistedTypes,
                                         Map<String, List<StreamConsumer>> streamConsumers,
                                         ScalableTopicStats stats) {
        // Every subscription known anywhere: recorded in the metadata store, held by the
        // controller, or present as a cursor on some segment (a QUEUE subscription created by
        // a consumer lives only on the segments).
        Map<String, ScalableTopicStats.SubscriptionStats> subscriptions = new LinkedHashMap<>();
        for (String name : persistedTypes.keySet()) {
            subscriptions.put(name, new ScalableTopicStats.SubscriptionStats());
        }
        for (String name : streamConsumers.keySet()) {
            subscriptions.computeIfAbsent(name, n -> new ScalableTopicStats.SubscriptionStats());
        }
        for (Long segmentId : layout.getAllSegments().keySet()) {
            TopicStats ts = segmentStats.get(segmentId);
            if (ts == null) {
                continue;
            }
            for (String name : ts.getSubscriptions().keySet()) {
                subscriptions.computeIfAbsent(name, n -> new ScalableTopicStats.SubscriptionStats());
            }
        }

        for (Map.Entry<String, ScalableTopicStats.SubscriptionStats> entry : subscriptions.entrySet()) {
            String name = entry.getKey();
            ScalableTopicStats.SubscriptionStats sub = entry.getValue();
            List<StreamConsumer> sessions = streamConsumers.get(name);
            // A subscription the controller coordinates is STREAM by definition; one that
            // exists only as cursors on the segments is QUEUE.
            ScalableSubscriptionType persisted = persistedTypes.get(name);
            sub.setType(persisted != null ? persisted
                    : sessions != null ? ScalableSubscriptionType.STREAM : ScalableSubscriptionType.QUEUE);

            Map<String, ScalableTopicStats.ConsumerStats> consumers = new LinkedHashMap<>();
            if (sessions != null) {
                for (StreamConsumer session : sessions) {
                    ScalableTopicStats.ConsumerStats c = new ScalableTopicStats.ConsumerStats();
                    c.setConsumerName(session.consumerName());
                    c.setConnected(session.connected());
                    c.setSegmentIds(new ArrayList<>(session.assignedSegmentIds()));
                    consumers.put(session.consumerName(), c);
                }
            }
            for (Long segmentId : layout.getAllSegments().keySet()) {
                TopicStats ts = segmentStats.get(segmentId);
                SubscriptionStats ss = ts != null ? ts.getSubscriptions().get(name) : null;
                if (ss == null) {
                    continue;
                }
                addSegmentSubscription(sub, segmentId, ss);
                for (ConsumerStats consumer : ss.getConsumers()) {
                    addSegmentConsumer(consumers, segmentId, consumer);
                }
            }
            sub.setConsumers(new ArrayList<>(consumers.values()));
        }
        stats.setSubscriptions(subscriptions);
    }

    private static void addSegmentSubscription(ScalableTopicStats.SubscriptionStats sub, long segmentId,
                                               SubscriptionStats ss) {
        ScalableTopicStats.SegmentSubscriptionStats seg = new ScalableTopicStats.SegmentSubscriptionStats();
        seg.setMsgBacklog(ss.getMsgBacklog());
        seg.setBacklogSize(ss.getBacklogSize());
        seg.setUnackedMessages(ss.getUnackedMessages());
        seg.setMsgRateOut(ss.getMsgRateOut());
        seg.setMsgThroughputOut(ss.getMsgThroughputOut());
        seg.setConsumerCount(ss.getConsumers().size());
        sub.getSegments().put(segmentId, seg);

        sub.setMsgBacklog(sub.getMsgBacklog() + ss.getMsgBacklog());
        sub.setBacklogSize(sub.getBacklogSize() + ss.getBacklogSize());
        sub.setUnackedMessages(sub.getUnackedMessages() + ss.getUnackedMessages());
        sub.setMsgRateOut(sub.getMsgRateOut() + ss.getMsgRateOut());
        sub.setMsgThroughputOut(sub.getMsgThroughputOut() + ss.getMsgThroughputOut());
        sub.setMsgRateRedeliver(sub.getMsgRateRedeliver() + ss.getMsgRateRedeliver());
        sub.setMessageAckRate(sub.getMessageAckRate() + ss.getMessageAckRate());
        sub.setMsgOutCounter(sub.getMsgOutCounter() + ss.getMsgOutCounter());
        sub.setBytesOutCounter(sub.getBytesOutCounter() + ss.getBytesOutCounter());
    }

    private static void addSegmentConsumer(Map<String, ScalableTopicStats.ConsumerStats> consumers,
                                           long segmentId, ConsumerStats consumer) {
        String name = stripSegmentSuffix(consumer.getConsumerName(), segmentId);
        ScalableTopicStats.ConsumerStats c = consumers.computeIfAbsent(name, n -> {
            ScalableTopicStats.ConsumerStats created = new ScalableTopicStats.ConsumerStats();
            created.setConsumerName(n);
            return created;
        });
        // Attached to a segment means connected, whatever the session said.
        c.setConnected(true);
        if (!c.getSegmentIds().contains(segmentId)) {
            c.getSegmentIds().add(segmentId);
        }
        c.setMsgRateOut(c.getMsgRateOut() + consumer.getMsgRateOut());
        c.setMsgThroughputOut(c.getMsgThroughputOut() + consumer.getMsgThroughputOut());
        c.setMsgOutCounter(c.getMsgOutCounter() + consumer.getMsgOutCounter());
        c.setBytesOutCounter(c.getBytesOutCounter() + consumer.getBytesOutCounter());
        c.setUnackedMessages(c.getUnackedMessages() + consumer.getUnackedMessages());
        c.setAvailablePermits(c.getAvailablePermits() + consumer.getAvailablePermits());
        if (c.getAddress() == null) {
            c.setAddress(consumer.getAddress());
            c.setConnectedSince(consumer.getConnectedSince());
            c.setClientVersion(consumer.getClientVersion());
        }
    }

    /**
     * Fold an underlying per-segment producer/consumer name back to the V5 name it was
     * derived from: {@code <name>-seg-<segmentId>} becomes {@code <name>}. Any other name is
     * returned unchanged.
     */
    static String stripSegmentSuffix(String name, long segmentId) {
        if (name == null) {
            return null;
        }
        String suffix = SEGMENT_SUFFIX + segmentId;
        if (name.length() > suffix.length() && name.endsWith(suffix)) {
            return name.substring(0, name.length() - suffix.length());
        }
        return name;
    }

    private static double averageSize(double msgRate, double byteRate) {
        return msgRate > 0 ? byteRate / msgRate : 0;
    }
}
