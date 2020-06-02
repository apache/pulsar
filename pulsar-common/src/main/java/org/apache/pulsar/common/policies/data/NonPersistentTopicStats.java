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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

/**
 * Statistics for a non-persistent topic.
 */
public class NonPersistentTopicStats extends TopicStats {

    /**
     * for non-persistent topic: broker drops msg if publisher publishes messages more than configured max inflight
     * messages per connection.
     **/
    public double msgDropRate;

    /** List of connected publishers on this topic w/ their stats. */
    public List<NonPersistentPublisherStats> publishers;

    /** Map of subscriptions with their individual statistics. */
    public Map<String, NonPersistentSubscriptionStats> subscriptions;

    /** Map of replication statistics by remote cluster context. */
    public Map<String, NonPersistentReplicatorStats> replication;

    public NonPersistentTopicStats() {
        this.publishers = Lists.newArrayList();
        this.subscriptions = Maps.newHashMap();
        this.replication = Maps.newTreeMap();
    }

    public void reset() {
        super.reset();
        this.msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats.
    public NonPersistentTopicStats add(NonPersistentTopicStats stats) {
        checkNotNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        return this;
    }

    public List<NonPersistentPublisherStats> getPublishers() {
        return this.publishers;
    }

    public Map<String, NonPersistentSubscriptionStats> getSubscriptions() {
        return this.subscriptions;
    }

    public Map<String, NonPersistentReplicatorStats> getReplication() {
        return this.replication;
    }
}
