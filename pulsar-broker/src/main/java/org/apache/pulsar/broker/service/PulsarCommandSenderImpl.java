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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

@Slf4j
public class PulsarCommandSenderImpl implements PulsarCommandSender {

    private final BrokerInterceptor interceptor;
    private final ServerCnx cnx;

    public PulsarCommandSenderImpl(BrokerInterceptor interceptor, ServerCnx cnx) {
        this.interceptor = interceptor;
        this.cnx = cnx;
    }

    @Override
    public void sendPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(partitions, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSuccessResponse(long requestId) {
        PulsarApi.BaseCommand command = Commands.newSuccessCommand(requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendErrorResponse(long requestId, PulsarApi.ServerError error, String message) {
        PulsarApi.BaseCommand command = Commands.newErrorCommand(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getError().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                            SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, lastSequenceId,
                schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                        long entryId) {
        PulsarApi.BaseCommand command = Commands.newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId,
                entryId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendReceipt().getMessageId().recycle();
        command.getSendReceipt().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        PulsarApi.BaseCommand command = Commands.newSendErrorCommand(producerId, sequenceId, error, errorMsg);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendError().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        PulsarApi.BaseCommand command = Commands.newGetTopicsOfNamespaceResponseCommand(topics, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetTopicsOfNamespaceResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseCommand(requestId, schema, version);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseCommand(requestId, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {
        PulsarApi.BaseCommand command = Commands.newConnectedCommand(clientProtocolVersion, maxMessageSize);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getConnected().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                                   PulsarApi.CommandLookupTopicResponse.LookupType response, long requestId, boolean proxyThroughServiceUrl) {
        PulsarApi.BaseCommand command = Commands.newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls,
                authoritative, response, requestId, proxyThroughServiceUrl);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.BaseCommand command = Commands.newLookupErrorResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
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
    public void sendError(long requestId, PulsarApi.ServerError error, String message) {
        cnx.ctx().writeAndFlush(Commands.newError(requestId, error, message));
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {
        // Only send notification if the client understand the command
        if (cnx.getRemoteEndpointProtocolVersion() >= PulsarApi.ProtocolVersion.v9_VALUE) {
            log.info("[{}] Notifying consumer that end of topic has been reached", this);
            cnx.ctx().writeAndFlush(Commands.newReachedEndOfTopic(consumerId));
        }
    }

    @Override
    public ChannelPromise sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, final List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker) {
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

                MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
                MessageIdData messageId = messageIdBuilder
                        .setLedgerId(entry.getLedgerId())
                        .setEntryId(entry.getEntryId())
                        .setPartition(partitionIdx)
                        .build();

                ByteBuf metadataAndPayload = entry.getDataBuffer();
                // increment ref-count of data and release at the end of process: so, we can get chance to call entry.release
                metadataAndPayload.retain();
                // skip checksum by incrementing reader-index if consumer-client doesn't support checksum verification
                if (cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v11.getNumber()) {
                    Commands.skipChecksumIfPresent(metadataAndPayload);
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Sending message to consumerId {}, msg id {}-{}", topicName, subscription,
                            consumerId, entry.getLedgerId(), entry.getEntryId());
                }

                int redeliveryCount = 0;
                PositionImpl position = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                if (redeliveryTracker.contains(position)) {
                    redeliveryCount = redeliveryTracker.incrementAndGetRedeliveryCount(position);
                }
                ctx.write(cnx.newMessageAndIntercept(consumerId, messageId, redeliveryCount, metadataAndPayload,
                        batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i), topicName), ctx.voidPromise());
                messageId.recycle();
                messageIdBuilder.recycle();
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

    private void safeIntercept(PulsarApi.BaseCommand command, ServerCnx cnx) {
        try {
            this.interceptor.onPulsarCommand(command, cnx);
        } catch (Exception e) {
            log.error("Failed to execute command {} on broker interceptor.", command.getType(), e);
        }
    }
}
