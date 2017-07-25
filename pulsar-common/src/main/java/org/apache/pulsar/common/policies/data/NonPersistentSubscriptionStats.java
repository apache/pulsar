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

import java.util.List;

import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;

import com.google.common.collect.Lists;

/**
 */
public class NonPersistentSubscriptionStats extends PersistentSubscriptionStats{
    
    /**
     * for non-persistent topic: broker drops msg for subscription if none of the consumer available for message
     * delivery
     **/
    public double msgDropRate;

    public NonPersistentSubscriptionStats() {
        this.consumers = Lists.newArrayList();
    }

    public void reset() {
        super.reset();
        msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats
    public NonPersistentSubscriptionStats add(NonPersistentSubscriptionStats stats) {
        checkNotNull(stats);
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.msgRateRedeliver += stats.msgRateRedeliver;
        this.msgBacklog += stats.msgBacklog;
        this.unackedMessages += stats.unackedMessages;
        this.msgRateExpired += stats.msgRateExpired;
        this.msgDropRate += stats.msgDropRate;
        if (this.consumers.size() != stats.consumers.size()) {
            for (int i = 0; i < stats.consumers.size(); i++) {
                ConsumerStats consumerStats = new ConsumerStats();
                this.consumers.add(consumerStats.add(stats.consumers.get(i)));
            }
        } else {
            for (int i = 0; i < stats.consumers.size(); i++) {
                this.consumers.get(i).add(stats.consumers.get(i));
            }
        }
        return this;
    }
}
