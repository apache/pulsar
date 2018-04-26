package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.utils.StatsOutputStream;

public class StreamingStats {
    private StreamingStats() {}

    public static void writePublisherStats(StatsOutputStream statsStream, PublisherStats stats) {
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


    public static void writeConsumerStats(StatsOutputStream statsStream, PulsarApi.CommandSubscribe.SubType subType,
        ConsumerStats stats) {
        // Populate consumer specific stats here
        statsStream.startObject();

        statsStream.writePair("address", stats.getAddress());
        statsStream.writePair("consumerName", stats.consumerName);
        statsStream.writePair("availablePermits", stats.availablePermits);
        statsStream.writePair("connectedSince", stats.getConnectedSince());
        statsStream.writePair("msgRateOut", stats.msgRateOut);
        statsStream.writePair("msgThroughputOut", stats.msgThroughputOut);
        statsStream.writePair("msgRateRedeliver", stats.msgRateRedeliver);

        if (PulsarApi.CommandSubscribe.SubType.Shared.equals(subType)) {
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
