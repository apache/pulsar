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
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

public interface PulsarCommandSender {

    void sendPartitionMetadataResponse(ServerError error, String errorMsg, long requestId);

    void sendPartitionMetadataResponse(int partitions, long requestId);

    void sendSuccessResponse(long requestId);

    void sendErrorResponse(long requestId, ServerError error, String message);

    void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion);

    void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                     SchemaVersion schemaVersion, Optional<Long> topicEpoch,
                                     boolean isProducerReady);

    void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                 long entryId);

    void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg);

    void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId);

    void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version);

    void sendGetSchemaErrorResponse(long requestId, ServerError error, String errorMessage);

    void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion);

    void sendGetOrCreateSchemaErrorResponse(long requestId, ServerError error, String errorMessage);

    void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize);

    void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                            CommandLookupTopicResponse.LookupType response, long requestId,
                            boolean proxyThroughServiceUrl);

    void sendLookupResponse(ServerError error, String errorMsg, long requestId);

    void sendActiveConsumerChange(long consumerId, boolean isActive);

    void sendSuccess(long requestId);

    void sendError(long requestId, ServerError error, String message);

    void sendReachedEndOfTopic(long consumerId);

    Future<Void> sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker);

    void sendTcClientConnectResponse(long requestId, ServerError error, String message);

    void sendTcClientConnectResponse(long requestId);

}
