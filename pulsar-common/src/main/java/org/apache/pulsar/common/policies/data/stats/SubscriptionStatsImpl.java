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

import lombok.Data;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics about subscription.
 */
@Data
public class SubscriptionStatsImpl implements SubscriptionStats {
    /** Total rate of messages delivered on this subscription (msg/s). */
    public double msgRateOut;

    /** Total throughput delivered on this subscription (bytes/s). */
    public double msgThroughputOut;

    /** Total bytes delivered to consumer (bytes). */
    public long bytesOutCounter;

    /** Total messages delivered to consumer (msg). */
    public long msgOutCounter;

    /** Total rate of messages redelivered on this subscription (msg/s). */
    public double msgRateRedeliver;

    /** Chunked message dispatch rate. */
    public int chunkedMessageRate;

    /** Number of messages in the subscription backlog. */
    public long msgBacklog;

    /** Size of backlog in byte. **/
    public long backlogSize;

    /** Number of messages in the subscription backlog that do not contain the delay messages. */
    public long msgBacklogNoDelayed;

    /** Flag to verify if subscription is blocked due to reaching threshold of unacked messages. */
    public boolean blockedSubscriptionOnUnackedMsgs;

    /** Number of delayed messages currently being tracked. */
    public long msgDelayed;

    /** Number of unacknowledged messages for the subscription. */
    public long unackedMessages;

    /** Whether this subscription is Exclusive or Shared or Failover. */
    public String type;

    /** The name of the consumer that is active for single active consumer subscriptions i.e. failover or exclusive. */
    public String activeConsumerName;

    /** Total rate of messages expired on this subscription (msg/s). */
    public double msgRateExpired;

    /** Total messages expired on this subscription. */
    public long totalMsgExpired;

    /** Last message expire execution timestamp. */
    public long lastExpireTimestamp;

    /** Last received consume flow command timestamp. */
    public long lastConsumedFlowTimestamp;

    /** Last consume message timestamp. */
    public long lastConsumedTimestamp;

    /** Last acked message timestamp. */
    public long lastAckedTimestamp;

    /** Last MarkDelete position advanced timesetamp. */
    public long lastMarkDeleteAdvancedTimestamp;

    /** List of connected consumers on this subscription w/ their stats. */
    public List<ConsumerStatsImpl> consumers;

    /** Tells whether this subscription is durable or ephemeral (eg.: from a reader). */
    public boolean isDurable;

    /** Mark that the subscription state is kept in sync across different regions. */
    public boolean isReplicated;

    /** Whether out of order delivery is allowed on the Key_Shared subscription. */
    public boolean allowOutOfOrderDelivery;

    /** Whether the Key_Shared subscription mode is AUTO_SPLIT or STICKY. */
    public String keySharedMode;

    /** This is for Key_Shared subscription to get the recentJoinedConsumers in the Key_Shared subscription. */
    public Map<String, String> consumersAfterMarkDeletePosition;

    /** The number of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRanges;

    /** The serialized size of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRangesSerializedSize;

    public SubscriptionStatsImpl() {
        this.consumers = new ArrayList<>();
        this.consumersAfterMarkDeletePosition = new LinkedHashMap<>();
    }

    public void reset() {
        msgRateOut = 0;
        msgThroughputOut = 0;
        bytesOutCounter = 0;
        msgOutCounter = 0;
        msgRateRedeliver = 0;
        msgBacklog = 0;
        backlogSize = 0;
        msgBacklogNoDelayed = 0;
        unackedMessages = 0;
        msgRateExpired = 0;
        totalMsgExpired = 0;
        lastExpireTimestamp = 0L;
        lastMarkDeleteAdvancedTimestamp = 0L;
        consumers.clear();
        consumersAfterMarkDeletePosition.clear();
        nonContiguousDeletedMessagesRanges = 0;
        nonContiguousDeletedMessagesRangesSerializedSize = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats
    public SubscriptionStatsImpl add(SubscriptionStatsImpl stats) {
        Objects.requireNonNull(stats);
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.bytesOutCounter += stats.bytesOutCounter;
        this.msgOutCounter += stats.msgOutCounter;
        this.msgRateRedeliver += stats.msgRateRedeliver;
        this.msgBacklog += stats.msgBacklog;
        this.backlogSize += stats.backlogSize;
        this.msgBacklogNoDelayed += stats.msgBacklogNoDelayed;
        this.msgDelayed += stats.msgDelayed;
        this.unackedMessages += stats.unackedMessages;
        this.msgRateExpired += stats.msgRateExpired;
        this.totalMsgExpired += stats.totalMsgExpired;
        this.isReplicated |= stats.isReplicated;
        this.isDurable |= stats.isDurable;
        if (this.consumers.size() != stats.consumers.size()) {
            for (int i = 0; i < stats.consumers.size(); i++) {
                ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
                this.consumers.add(consumerStats.add(stats.consumers.get(i)));
            }
        } else {
            for (int i = 0; i < stats.consumers.size(); i++) {
                this.consumers.get(i).add(stats.consumers.get(i));
            }
        }
        this.allowOutOfOrderDelivery |= stats.allowOutOfOrderDelivery;
        this.consumersAfterMarkDeletePosition.putAll(stats.consumersAfterMarkDeletePosition);
        this.nonContiguousDeletedMessagesRanges += stats.nonContiguousDeletedMessagesRanges;
        this.nonContiguousDeletedMessagesRangesSerializedSize += stats.nonContiguousDeletedMessagesRangesSerializedSize;
        return this;
    }
}
