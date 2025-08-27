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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.prometheus.client.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;

@Slf4j
public class PulsarCommandSenderImpl implements PulsarCommandSender {
    private static final Counter PROCESS_FAILURE_COUNTER = Counter.build()
            .name("pulsar_broker_process_request_failures")
            .help("Number of send error responses sent by the broker")
            .labelNames("error")
            .register();

    private final BrokerInterceptor interceptor;
    private final ServerCnx cnx;

    public PulsarCommandSenderImpl(BrokerInterceptor interceptor, ServerCnx cnx) {
        this.interceptor = interceptor;
        this.cnx = cnx;
    }

    @Override
    public void sendPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newPartitionMetadataResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {
        BaseCommand command = Commands.newPartitionMetadataResponseCommand(partitions, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendSuccessResponse(long requestId) {
        BaseCommand command = Commands.newSuccessCommand(requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendErrorResponse(long requestId, ServerError error, String message) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newErrorCommand(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
        BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                            SchemaVersion schemaVersion, Optional<Long> topicEpoch,
                                            boolean isProducerReady) {
        BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, lastSequenceId,
                schemaVersion, topicEpoch, isProducerReady);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                        long entryId) {
        BaseCommand command = Commands.newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId,
                entryId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newSendErrorCommand(producerId, sequenceId, error, errorMsg);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, String topicsHash,
                                                 boolean filtered, boolean changed, long requestId) {
        BaseCommand command = Commands.newGetTopicsOfNamespaceResponseCommand(topics, topicsHash,
                filtered, changed, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        BaseCommand command = Commands.newGetSchemaResponseCommand(requestId, schema, version);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newGetSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        BaseCommand command = Commands.newGetOrCreateSchemaResponseCommand(requestId, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command =
                Commands.newGetOrCreateSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize, boolean supportsTopicWatchers) {
        BaseCommand command = Commands.newConnectedCommand(
                clientProtocolVersion, maxMessageSize, supportsTopicWatchers);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                                   CommandLookupTopicResponse.LookupType response,
                                   long requestId, boolean proxyThroughServiceUrl) {
        BaseCommand command = Commands.newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls,
                authoritative, response, requestId, proxyThroughServiceUrl);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(ServerError error, String errorMsg, long requestId) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newLookupErrorResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {
        if (!Commands.peerSupportsActiveConsumerListener(cnx.getRemoteEndpointProtocolVersion())) {
            // if the client is older than `v12`, we don't need to send consumer group changes.
            return;
        }
        writeAndFlush(Commands.newActiveConsumerChange(consumerId, isActive));
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {
        // Only send notification if the client understand the command
        if (cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v9.getValue()) {
            log.info("[{}] Notifying consumer that end of topic has been reached", this);
            writeAndFlush(Commands.newReachedEndOfTopic(consumerId));
        }
    }

    @Override
    public boolean sendTopicMigrated(ResourceType type, long resourceId, String brokerUrl, String brokerUrlTls) {
        // Only send notification if the client understand the command
        if (cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v20.getValue()) {
            log.info("[{}] Notifying {} that topic is migrated", type.name(), resourceId);
            writeAndFlush(Commands.newTopicMigrated(type, resourceId, brokerUrl, brokerUrlTls));
            return true;
        }
        return false;
    }

    @Override
    public ChannelPromise sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
                                                 int partitionIdx, List<? extends Entry> entries,
                                                 EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
                                                 RedeliveryTracker redeliveryTracker, long epoch) {
        final ChannelHandlerContext ctx = cnx.ctx();
        final ChannelPromise writePromise = ctx.newPromise();
        ctx.channel().eventLoop().execute(() -> {
            // this list is always accessed in the same thread (the eventLoop here)
            // and in the completion of the writePromise
            // it is safe to use a simple ArrayList
            List<Entry> entriesToRelease = new ArrayList<>(entries.size());
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

                int redeliveryCount = redeliveryTracker
                        .getRedeliveryCount(entry.getLedgerId(), entry.getEntryId());

                ctx.write(
                        cnx.newMessageAndIntercept(consumerId, entry.getLedgerId(), entry.getEntryId(), partitionIdx,
                                redeliveryCount, metadataAndPayload,
                                batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i), topicName, epoch),
                        ctx.voidPromise());
                entriesToRelease.add(entry);
            }

            // Use an empty write here so that we can just tie the flush with the write promise for last entry
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            writePromise.addListener((future) -> {
                // release the entries only after flushing the channel
                //
                // InflightReadsLimiter tracks the amount of memory retained by in-flight data to the
                // consumer. It counts the memory as being released when the entry is deallocated
                // that is that it reaches refcnt=0.
                // so we need to call release only when we are sure that Netty released the internal ByteBuf
                entriesToRelease.forEach(Entry::release);
            });
            batchSizes.recyle();
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
        });

        return writePromise;
    }

    @Override
    public void sendTcClientConnectResponse(long requestId, ServerError error, String message) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newTcClientConnectResponse(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendTcClientConnectResponse(long requestId) {
        sendTcClientConnectResponse(requestId, null, null);
    }

    @Override
    public void sendNewTxnResponse(long requestId, TxnID txnID, long tcID) {
        BaseCommand command = Commands.newTxnResponse(requestId, txnID.getLeastSigBits(),
                txnID.getMostSigBits());
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
        if (this.interceptor != null) {
            this.interceptor.txnOpened(tcID, txnID.toString());
        }
    }

    @Override
    public void sendNewTxnErrorResponse(long requestId, long tcID, ServerError error, String message) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newTxnResponse(requestId, tcID, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    @Override
    public void sendEndTxnResponse(long requestId, TxnID txnID, int txnAction) {
        BaseCommand command = Commands.newEndTxnResponse(requestId, txnID.getLeastSigBits(),
                txnID.getMostSigBits());
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
        if (this.interceptor != null) {
            this.interceptor.txnEnded(txnID.toString(), txnAction);
        }
    }

    @Override
    public void sendEndTxnErrorResponse(long requestId, TxnID txnID, ServerError error, String message) {
        if (error != null) {
            PROCESS_FAILURE_COUNTER.labels(error.name()).inc();
        }
        BaseCommand command = Commands.newEndTxnResponse(requestId, txnID.getLeastSigBits(),
                txnID.getMostSigBits(), error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    /***
     * @param topics topic names which are matching, the topic name contains the partition suffix.
     */
    @Override
    public void sendWatchTopicListSuccess(long requestId, long watcherId, String topicsHash, List<String> topics) {
        BaseCommand command = Commands.newWatchTopicListSuccess(requestId, watcherId, topicsHash, topics);
        interceptAndWriteCommand(command);
    }

    /***
     * {@inheritDoc}
     */
    @Override
    public void sendWatchTopicListUpdate(long watcherId,
                                         List<String> newTopics, List<String> deletedTopics, String topicsHash) {
        BaseCommand command = Commands.newWatchTopicUpdate(watcherId, newTopics, deletedTopics, topicsHash);
        interceptAndWriteCommand(command);
    }

    private void interceptAndWriteCommand(BaseCommand command) {
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        writeAndFlush(outBuf);
    }

    private void writeAndFlush(ByteBuf outBuf) {
        NettyChannelUtil.writeAndFlushWithVoidPromise(cnx.ctx(), outBuf);
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
