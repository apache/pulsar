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
package org.apache.pulsar.common.policies.data.stats;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;

/**
 * Statistics for a non-persistent topic.
 * This class is not thread-safe.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class NonPersistentTopicStatsImpl extends TopicStatsImpl implements NonPersistentTopicStats {

    /**
     * for non-persistent topic: broker drops msg if publisher publishes messages more than configured max inflight
     * messages per connection.
     **/
    @Getter
    public double msgDropRate;

    @JsonIgnore
    public List<PublisherStatsImpl> publishers;

    @JsonIgnore
    public Map<String, SubscriptionStatsImpl> subscriptions;

    @JsonIgnore
    public Map<String, ReplicatorStatsImpl> replication;

    @JsonProperty("publishers")
    public List<NonPersistentPublisherStats> getNonPersistentPublishers() {
        return Stream.concat(nonPersistentPublishers.stream().sorted(
                        Comparator.comparing(NonPersistentPublisherStats::getProducerName, nullsLast(naturalOrder()))),
                nonPersistentPublishersMap.values().stream().sorted(
                        Comparator.comparing(NonPersistentPublisherStats::getProducerName, nullsLast(naturalOrder()))))
                .collect(Collectors.toList());
    }

    @JsonProperty("subscriptions")
    public Map<String, NonPersistentSubscriptionStats> getNonPersistentSubscriptions() {
        return (Map<String, NonPersistentSubscriptionStats>) nonPersistentSubscriptions;
    }

    @JsonProperty("replication")
    public Map<String, NonPersistentReplicatorStats> getNonPersistentReplicators() {
        return (Map<String, NonPersistentReplicatorStats>) nonPersistentReplicators;
    }

    /** List of connected publishers on this non-persistent topic w/ their stats. */
    private List<NonPersistentPublisherStats> nonPersistentPublishers;

    private Map<String, NonPersistentPublisherStats> nonPersistentPublishersMap;

    /** Map of non-persistent subscriptions with their individual statistics. */
    public Map<String, ? extends NonPersistentSubscriptionStats> nonPersistentSubscriptions;

    /** Map of non-persistent replication statistics by remote cluster context. */
    public Map<String, ? extends NonPersistentReplicatorStats> nonPersistentReplicators;

    @SuppressFBWarnings(value = "MF_CLASS_MASKS_FIELD", justification = "expected to override")
    public List<NonPersistentPublisherStats> getPublishers() {
        return Stream.concat(nonPersistentPublishers.stream().sorted(
                        Comparator.comparing(NonPersistentPublisherStats::getProducerName, nullsLast(naturalOrder()))),
                nonPersistentPublishersMap.values().stream().sorted(
                        Comparator.comparing(NonPersistentPublisherStats::getProducerName, nullsLast(naturalOrder()))))
                .collect(Collectors.toList());
    }

    public void setPublishers(List<? extends PublisherStats> statsList) {
        this.nonPersistentPublishers.clear();
        this.nonPersistentPublishersMap.clear();
        statsList.forEach(s -> addPublisher((NonPersistentPublisherStatsImpl) s));
    }

    public void addPublisher(NonPersistentPublisherStatsImpl stats) {
        if (stats.isSupportsPartialProducer() && stats.getProducerName() != null) {
            nonPersistentPublishersMap.put(stats.getProducerName(), stats);
        } else {
            stats.setSupportsPartialProducer(false); // setter method with side effect
            nonPersistentPublishers.add(stats);
        }
    }

    @SuppressFBWarnings(value = "MF_CLASS_MASKS_FIELD", justification = "expected to override")
    public Map<String, NonPersistentSubscriptionStats> getSubscriptions() {
        return (Map<String, NonPersistentSubscriptionStats>) nonPersistentSubscriptions;
    }

    @SuppressFBWarnings(value = "MF_CLASS_MASKS_FIELD", justification = "expected to override")
    public Map<String, NonPersistentReplicatorStats> getReplication() {
        return (Map<String, NonPersistentReplicatorStats>) nonPersistentReplicators;
    }

    @Override
    public double getMsgDropRate() {
        return msgDropRate;
    }

    public NonPersistentTopicStatsImpl() {
        this.nonPersistentPublishers = new ArrayList<>();
        this.nonPersistentPublishersMap = new ConcurrentHashMap<>();
        this.nonPersistentSubscriptions = new HashMap<>();
        this.nonPersistentReplicators = new TreeMap<>();
    }

    public void reset() {
        super.reset();
        this.nonPersistentPublishers.clear();
        this.nonPersistentPublishersMap.clear();
        this.nonPersistentSubscriptions.clear();
        this.nonPersistentReplicators.clear();
        this.msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats. This stat addition is not thread-safe.
    public NonPersistentTopicStatsImpl add(NonPersistentTopicStats ts) {
        NonPersistentTopicStatsImpl stats = (NonPersistentTopicStatsImpl) ts;
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        List<NonPersistentPublisherStats> publisherStats = stats.getNonPersistentPublishers();
        for (int index = 0; index < publisherStats.size(); index++) {
            NonPersistentPublisherStats s = publisherStats.get(index);
            if (s.isSupportsPartialProducer() && s.getProducerName() != null) {
                ((NonPersistentPublisherStatsImpl) this.nonPersistentPublishersMap
                        .computeIfAbsent(s.getProducerName(), key -> {
                            final NonPersistentPublisherStatsImpl newStats = new NonPersistentPublisherStatsImpl();
                            newStats.setSupportsPartialProducer(true);
                            newStats.setProducerName(s.getProducerName());
                            return newStats;
                        })).add((NonPersistentPublisherStatsImpl) s);
            } else {
                // Add a non-persistent publisher stat entry to this.nonPersistentPublishers
                // if this.nonPersistentPublishers.size() is smaller than
                // the input stats.nonPersistentPublishers.size().
                // Here, index == this.nonPersistentPublishers.size() means
                // this.nonPersistentPublishers.size() is smaller than the input stats.nonPersistentPublishers.size()
                if (index == this.nonPersistentPublishers.size()) {
                    NonPersistentPublisherStatsImpl newStats = new NonPersistentPublisherStatsImpl();
                    newStats.setSupportsPartialProducer(false);
                    this.nonPersistentPublishers.add(newStats);
                }
                ((NonPersistentPublisherStatsImpl) this.nonPersistentPublishers.get(index))
                        .add((NonPersistentPublisherStatsImpl) s);
            }
        }

        for (Map.Entry<String, NonPersistentSubscriptionStats> entry : stats.getNonPersistentSubscriptions()
                .entrySet()) {
            NonPersistentSubscriptionStatsImpl subscriptionStats =
                    (NonPersistentSubscriptionStatsImpl) this.getNonPersistentSubscriptions()
                            .computeIfAbsent(entry.getKey(), k -> new NonPersistentSubscriptionStatsImpl());
            subscriptionStats.add(
                    (NonPersistentSubscriptionStatsImpl) entry.getValue());
        }

        for (Map.Entry<String, NonPersistentReplicatorStats> entry : stats.getNonPersistentReplicators().entrySet()) {
            NonPersistentReplicatorStatsImpl replStats = (NonPersistentReplicatorStatsImpl)
                    this.getNonPersistentReplicators().computeIfAbsent(entry.getKey(), k -> {
                        NonPersistentReplicatorStatsImpl r = new NonPersistentReplicatorStatsImpl();
                        return r;
                    });
            replStats.add((NonPersistentReplicatorStatsImpl) entry.getValue());
        }

        return this;
    }

}
