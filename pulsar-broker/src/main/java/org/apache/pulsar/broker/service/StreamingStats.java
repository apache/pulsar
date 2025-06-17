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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.utils.StatsOutputStream;

public class StreamingStats {
    private StreamingStats() {}

    public static void writePublisherStats(StatsOutputStream statsStream, PublisherStatsImpl stats) {
        statsStream.startObject();

        statsStream.writePair("msgRateIn", stats.getMsgRateIn());
        statsStream.writePair("msgThroughputIn", stats.getMsgThroughputIn());
        statsStream.writePair("averageMsgSize", stats.getAverageMsgSize());

        statsStream.writePair("address", stats.getAddress());
        statsStream.writePair("producerId", stats.getProducerId());
        statsStream.writePair("producerName", stats.getProducerName());
        statsStream.writePair("connectedSince", stats.getConnectedSince());
        if (stats.getClientVersion() != null) {
            statsStream.writePair("clientVersion", stats.getClientVersion());
        }

        // add metadata
        statsStream.startObject("metadata");
        if (stats.getMetadata() != null && !stats.getMetadata().isEmpty()) {
            stats.getMetadata().forEach(statsStream::writePair);
        }
        statsStream.endObject();

        statsStream.endObject();
    }


    public static void writeConsumerStats(StatsOutputStream statsStream, CommandSubscribe.SubType subType,
        ConsumerStatsImpl stats) {
        // Populate consumer specific stats here
        statsStream.startObject();

        statsStream.writePair("address", stats.getAddress());
        statsStream.writePair("consumerName", stats.getConsumerName());
        statsStream.writePair("availablePermits", stats.getAvailablePermits());
        statsStream.writePair("connectedSince", stats.getConnectedSince());
        statsStream.writePair("msgRateOut", stats.getMsgRateOut());
        statsStream.writePair("msgThroughputOut", stats.getMsgThroughputOut());
        statsStream.writePair("msgRateRedeliver", stats.getMsgRateRedeliver());
        statsStream.writePair("avgMessagesPerEntry", stats.getAvgMessagesPerEntry());
        statsStream.writePair("messageAckRate", stats.getMessageAckRate());

        if (Subscription.isIndividualAckMode(subType)) {
            statsStream.writePair("unackedMessages", stats.getUnackedMessages());
            statsStream.writePair("blockedConsumerOnUnackedMsgs", stats.isBlockedConsumerOnUnackedMsgs());
        }
        if (stats.getClientVersion() != null) {
            statsStream.writePair("clientVersion", stats.getClientVersion());
        }

        // add metadata
        statsStream.startObject("metadata");
        if (stats.getMetadata() != null && !stats.getMetadata().isEmpty()) {
            stats.getMetadata().forEach(statsStream::writePair);
        }
        statsStream.endObject();

        statsStream.endObject();
    }
}
