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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
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

    public List<NonPersistentPublisherStats> getNonPersistentPublishers() {
        return (List<NonPersistentPublisherStats>) nonPersistentPublishers;
    }

    public Map<String, NonPersistentSubscriptionStats> getNonPersistentSubscriptions() {
        return (Map<String, NonPersistentSubscriptionStats>) nonPersistentSubscriptions;
    }

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
        this.msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats.
    public NonPersistentTopicStatsImpl add(NonPersistentTopicStatsImpl stats) {
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        return this;
    }

}
