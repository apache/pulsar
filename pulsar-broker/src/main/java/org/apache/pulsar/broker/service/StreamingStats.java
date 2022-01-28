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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.utils.StatsOutputStream;

public class StreamingStats {
    private StreamingStats() {}

    public static void writePublisherStats(StatsOutputStream statsStream, PublisherStatsImpl stats) {
        statsStream.startObject();

        statsStream.writePair("msgRateIn", stats.msgRateIn);
        statsStream.writePair("msgThroughputIn", stats.msgThroughputIn);
        statsStream.writePair("averageMsgSize", stats.averageMsgSize);

        statsStream.writePair("address", stats.getAddress());
        statsStream.writePair("producerId", stats.producerId);
        statsStream.writePair("producerName", stats.getProducerName());
        statsStream.writePair("connectedSince", stats.getConnectedSince());
        if (stats.getClientVersion() != null) {
            statsStream.writePair("clientVersion", stats.getClientVersion());
        }

        // add metadata
        statsStream.startObject("metadata");
        if (stats.metadata != null && !stats.metadata.isEmpty()) {
            stats.metadata.forEach(statsStream::writePair);
        }
        statsStream.endObject();

        statsStream.endObject();
    }


    public static void writeConsumerStats(StatsOutputStream statsStream, CommandSubscribe.SubType subType,
        ConsumerStatsImpl stats) {
        // Populate consumer specific stats here
        statsStream.startObject();

        statsStream.writePair("address", stats.getAddress());
        statsStream.writePair("consumerName", stats.consumerName);
        statsStream.writePair("availablePermits", stats.availablePermits);
        statsStream.writePair("connectedSince", stats.getConnectedSince());
        statsStream.writePair("msgRateOut", stats.msgRateOut);
        statsStream.writePair("msgThroughputOut", stats.msgThroughputOut);
        statsStream.writePair("msgRateRedeliver", stats.msgRateRedeliver);
        statsStream.writePair("avgMessagesPerEntry", stats.avgMessagesPerEntry);

        if (Subscription.isIndividualAckMode(subType)) {
            statsStream.writePair("unackedMessages", stats.unackedMessages);
            statsStream.writePair("blockedConsumerOnUnackedMsgs", stats.blockedConsumerOnUnackedMsgs);
        }
        if (stats.getClientVersion() != null) {
            statsStream.writePair("clientVersion", stats.getClientVersion());
        }

        // add metadata
        statsStream.startObject("metadata");
        if (stats.metadata != null && !stats.metadata.isEmpty()) {
            stats.metadata.forEach(statsStream::writePair);
        }
        statsStream.endObject();

        statsStream.endObject();
    }
}
