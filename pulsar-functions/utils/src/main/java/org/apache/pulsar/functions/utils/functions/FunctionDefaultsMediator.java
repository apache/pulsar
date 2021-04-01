package org.apache.pulsar.functions.utils.functions;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.functions.proto.Function;

public interface FunctionDefaultsMediator {
    boolean isBatchingDisabled();
    boolean isChunkingEnabled();
    boolean isBlockIfQueueFullDisabled();
    CompressionType getCompressionType();
    Function.CompressionType getCompressionTypeProto();
    HashingScheme getHashingScheme();
    Function.HashingScheme getHashingSchemeProto();
    MessageRoutingMode getMessageRoutingMode();
    Function.MessageRoutingMode getMessageRoutingModeProto();
    Long getBatchingMaxPublishDelay();
}
