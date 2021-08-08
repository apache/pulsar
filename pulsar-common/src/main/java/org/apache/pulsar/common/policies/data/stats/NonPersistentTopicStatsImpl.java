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
package org.apache.pulsar.common.policies.data.stats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Statistics for a non-persistent topic.
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
        return (List<NonPersistentPublisherStats>) nonPersistentPublishers;
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
    public List<? extends NonPersistentPublisherStats> nonPersistentPublishers;

    /** Map of non-persistent subscriptions with their individual statistics. */
    public Map<String, ? extends NonPersistentSubscriptionStats> nonPersistentSubscriptions;

    /** Map of non-persistent replication statistics by remote cluster context. */
    public Map<String, ? extends NonPersistentReplicatorStats> nonPersistentReplicators;

    @SuppressFBWarnings(value = "MF_CLASS_MASKS_FIELD", justification = "expected to override")
    public List<NonPersistentPublisherStats> getPublishers() {
        return (List<NonPersistentPublisherStats>) nonPersistentPublishers;
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
        this.nonPersistentSubscriptions = new HashMap<>();
        this.nonPersistentReplicators = new TreeMap<>();
    }

    public void reset() {
        super.reset();
        this.nonPersistentPublishers.clear();
        this.nonPersistentSubscriptions.clear();
        this.nonPersistentReplicators.clear();
        this.msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats.
    public NonPersistentTopicStatsImpl add(NonPersistentTopicStats ts) {
        NonPersistentTopicStatsImpl stats = (NonPersistentTopicStatsImpl) ts;
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;

        if (this.getNonPersistentPublishers().size() != stats.getNonPersistentPublishers().size()) {
            for (int i = 0; i < stats.getNonPersistentPublishers().size(); i++) {
                NonPersistentPublisherStatsImpl publisherStats = new NonPersistentPublisherStatsImpl();
                this.getNonPersistentPublishers().add(publisherStats
                        .add((NonPersistentPublisherStatsImpl) stats.getNonPersistentPublishers().get(i)));
            }
        } else {
            for (int i = 0; i < stats.getNonPersistentPublishers().size(); i++) {
                ((NonPersistentPublisherStatsImpl) this.getNonPersistentPublishers().get(i))
                        .add((NonPersistentPublisherStatsImpl) stats.getNonPersistentPublishers().get(i));
            }
        }

        if (this.getNonPersistentSubscriptions().size() != stats.getNonPersistentSubscriptions().size()) {
            for (String subscription : stats.getNonPersistentSubscriptions().keySet()) {
                NonPersistentSubscriptionStatsImpl subscriptionStats = new NonPersistentSubscriptionStatsImpl();
                this.getNonPersistentSubscriptions().put(subscription, subscriptionStats
                        .add((NonPersistentSubscriptionStatsImpl) stats.getNonPersistentSubscriptions().get(subscription)));
            }
        } else {
            for (String subscription : stats.getNonPersistentSubscriptions().keySet()) {
                if (this.getNonPersistentSubscriptions().get(subscription) != null) {
                    ((NonPersistentSubscriptionStatsImpl) this.getNonPersistentSubscriptions().get(subscription))
                          .add((NonPersistentSubscriptionStatsImpl) stats.getNonPersistentSubscriptions().get(subscription));
                } else {
                    NonPersistentSubscriptionStatsImpl subscriptionStats = new NonPersistentSubscriptionStatsImpl();
                    this.getNonPersistentSubscriptions().put(subscription, subscriptionStats
                         .add((NonPersistentSubscriptionStatsImpl) stats.getNonPersistentSubscriptions().get(subscription)));
                }
            }
        }

        if (this.getNonPersistentReplicators().size() != stats.getNonPersistentReplicators().size()) {
            for (String repl : stats.getNonPersistentReplicators().keySet()) {
                NonPersistentReplicatorStatsImpl replStats = new NonPersistentReplicatorStatsImpl();
                this.getNonPersistentReplicators().put(repl, replStats
                        .add((NonPersistentReplicatorStatsImpl) stats.getNonPersistentReplicators().get(repl)));
            }
        } else {
            for (String repl : stats.getNonPersistentReplicators().keySet()) {
                if (this.getNonPersistentReplicators().get(repl) != null) {
                    ((NonPersistentReplicatorStatsImpl) this.getNonPersistentReplicators().get(repl))
                            .add((NonPersistentReplicatorStatsImpl) stats.getNonPersistentReplicators().get(repl));
                } else {
                    NonPersistentReplicatorStatsImpl replStats = new NonPersistentReplicatorStatsImpl();
                    this.getNonPersistentReplicators().put(repl, replStats
                            .add((NonPersistentReplicatorStatsImpl) stats.getNonPersistentReplicators().get(repl)));
                }
            }
        }
        return this;
    }

}
