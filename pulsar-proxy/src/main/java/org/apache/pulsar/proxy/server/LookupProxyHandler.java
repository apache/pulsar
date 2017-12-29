package org.apache.pulsar.proxy.server;

import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;

public interface LookupProxyHandler {
    public void handleLookup(CommandLookupTopic lookup);
    public void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata);
}
