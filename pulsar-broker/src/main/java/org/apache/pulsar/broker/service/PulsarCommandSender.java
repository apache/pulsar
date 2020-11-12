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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

public interface PulsarCommandSender {

    default void sendPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        // No-op
    }

    default void sendPartitionMetadataResponse(int partitions, long requestId) {
        // No-op
    }

    default void sendSuccessResponse(long requestId) {
        // No-op
    }

    default void sendErrorResponse(long requestId, PulsarApi.ServerError error, String message) {
        // No-op
    }

    default void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
        // No-op
    }

    default void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                     SchemaVersion schemaVersion) {
        // No-op
    }

    default void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                 long entryId) {
        // No-op
    }

    default void sendSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        // No-op
    }

    default void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        // No-op
    }

    default void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        // No-op
    }

    default void sendGetSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        // No-op
    }

    default void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        // No-op
    }

    default void sendGetOrCreateSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        // No-op
    }

    default void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {
        // No-op
    }

    default void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                            PulsarApi.CommandLookupTopicResponse.LookupType response, long requestId, boolean proxyThroughServiceUrl) {
        // No-op
    }

    default void sendLookupResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        // No-op
    }

    default void sendActiveConsumerChange(long consumerId, boolean isActive) {
        // No-op
    }

    default void sendSuccess(long requestId) {
        // No-op
    }

    default void sendError(long requestId, PulsarApi.ServerError error, String message) {
        // No-op
    }

    default void sendReachedEndOfTopic(long consumerId) {
        // No-op
    }

    default Future<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, final List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker) {
        return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
    }
}
