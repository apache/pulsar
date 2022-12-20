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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

@Slf4j
public class PulsarCommandSenderImpl implements PulsarCommandSender {

    private final BrokerInterceptor interceptor;
    private final ServerCnx cnx;

    public PulsarCommandSenderImpl(BrokerInterceptor interceptor, ServerCnx cnx) {
        this.interceptor = interceptor;
        this.cnx = cnx;
    }

    @Override
    public void sendPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {
        BaseCommand command = Commands.newPartitionMetadataResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {
        BaseCommand command = Commands.newPartitionMetadataResponseCommand(partitions, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSuccessResponse(long requestId) {
        BaseCommand command = Commands.newSuccessCommand(requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendErrorResponse(long requestId, ServerError error, String message) {
        BaseCommand command = Commands.newErrorCommand(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
        BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                            SchemaVersion schemaVersion, Optional<Long> topicEpoch,
                                            boolean isProducerReady) {
        BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, lastSequenceId,
                schemaVersion, topicEpoch, isProducerReady);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                        long entryId) {
        BaseCommand command = Commands.newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId,
                entryId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
        BaseCommand command = Commands.newSendErrorCommand(producerId, sequenceId, error, errorMsg);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        BaseCommand command = Commands.newGetTopicsOfNamespaceResponseCommand(topics, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        BaseCommand command = Commands.newGetSchemaResponseCommand(requestId, schema, version);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {
        BaseCommand command = Commands.newGetSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        BaseCommand command = Commands.newGetOrCreateSchemaResponseCommand(requestId, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {
        BaseCommand command =
                Commands.newGetOrCreateSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {
        BaseCommand command = Commands.newConnectedCommand(clientProtocolVersion, maxMessageSize);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                                   CommandLookupTopicResponse.LookupType response,
                                   long requestId, boolean proxyThroughServiceUrl) {
        BaseCommand command = Commands.newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls,
                authoritative, response, requestId, proxyThroughServiceUrl);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(ServerError error, String errorMsg, long requestId) {
        BaseCommand command = Commands.newLookupErrorResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {
        if (!Commands.peerSupportsActiveConsumerListener(cnx.getRemoteEndpointProtocolVersion())) {
            // if the client is older than `v12`, we don't need to send consumer group changes.
            return;
        }
        cnx.ctx().writeAndFlush(
                Commands.newActiveConsumerChange(consumerId, isActive),
                cnx.ctx().voidPromise());
    }

    @Override
    public void sendSuccess(long requestId) {
        cnx.ctx().writeAndFlush(Commands.newSuccess(requestId));
    }

    @Override
    public void sendError(long requestId, ServerError error, String message) {
        cnx.ctx().writeAndFlush(Commands.newError(requestId, error, message));
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {
        // Only send notification if the client understand the command
        if (cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v9.getValue()) {
            log.info("[{}] Notifying consumer that end of topic has been reached", this);
            cnx.ctx().writeAndFlush(Commands.newReachedEndOfTopic(consumerId));
        }
    }

    @Override
    public ChannelPromise sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker, long epoch) {
        final ChannelHandlerContext ctx = cnx.ctx();
        final ChannelPromise writePromise = ctx.newPromise();
        ctx.channel().eventLoop().execute(() -> {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry == null) {
                    // Entry was filtered out
                    continue;
                }

                int batchSize = batchSizes.getBatchSize(i);

                if (batchSize > 1 && !cnx.isBatchMessageCompatibleVersion()) {
                    log.warn("[{}-{}] Consumer doesn't support batch messages -  consumerId {}, msg id {}-{}",
                            topicName, subscription,
                            consumerId, entry.getLedgerId(), entry.getEntryId());
                    ctx.close();
                    entry.release();
                    continue;
                }

                ByteBuf metadataAndPayload = entry.getDataBuffer();
                // increment ref-count of data and release at the end of process:
                // so, we can get chance to call entry.release
                metadataAndPayload.retain();
                // skip broker entry metadata if consumer-client doesn't support broker entry metadata or the
                // features is not enabled
                if (cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v18.getValue()
                        || !cnx.supportBrokerMetadata()
                        || !cnx.getBrokerService().getPulsar().getConfig()
                        .isExposingBrokerEntryMetadataToClientEnabled()) {
                    Commands.skipBrokerEntryMetadataIfExist(metadataAndPayload);
                }
                // skip checksum by incrementing reader-index if consumer-client doesn't support checksum verification
                if (cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v11.getValue()) {
                    Commands.skipChecksumIfPresent(metadataAndPayload);
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Sending message to consumerId {}, msg id {}-{} with batchSize {}",
                            topicName, subscription,  consumerId, entry.getLedgerId(), entry.getEntryId(), batchSize);
                }

                int redeliveryCount = 0;
                PositionImpl position = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                if (redeliveryTracker.contains(position)) {
                    redeliveryCount = redeliveryTracker.incrementAndGetRedeliveryCount(position);
                }

                ctx.write(
                        cnx.newMessageAndIntercept(consumerId, entry.getLedgerId(), entry.getEntryId(), partitionIdx,
                                redeliveryCount, metadataAndPayload,
                                batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i), topicName, epoch),
                        ctx.voidPromise());
                entry.release();
            }

            // Use an empty write here so that we can just tie the flush with the write promise for last entry
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            batchSizes.recyle();
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
        });

        return writePromise;
    }

    @Override
    public void sendTcClientConnectResponse(long requestId, ServerError error, String message) {
        BaseCommand command = Commands.newTcClientConnectResponse(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendTcClientConnectResponse(long requestId) {
        sendTcClientConnectResponse(requestId, null, null);
    }

    private void safeIntercept(BaseCommand command, ServerCnx cnx) {
        if (this.interceptor != null) {
            try {
                this.interceptor.onPulsarCommand(command, cnx);
            } catch (Exception e) {
                log.error("Failed to execute command {} on broker interceptor.", command.getType(), e);
            }
        }
    }
}
