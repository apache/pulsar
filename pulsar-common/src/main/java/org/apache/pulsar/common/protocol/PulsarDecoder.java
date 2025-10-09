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
package org.apache.pulsar.common.protocol;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAckResponse;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxn;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxn;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandConsumerStats;
import org.apache.pulsar.common.api.proto.CommandConsumerStatsResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxn;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartition;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscription;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageId;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageIdResponse;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandNewTxn;
import org.apache.pulsar.common.api.proto.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandPong;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandReachedEndOfTopic;
import org.apache.pulsar.common.api.proto.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.CommandSeek;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectRequest;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectResponse;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.CommandWatchTopicList;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic implementation of the channel handler to process inbound Pulsar data.
 * <p>
 * Please be aware that the decoded protocol command instance passed to a handle* method is cleared and reused for the
 * next protocol command after the method completes. This is done in order to minimize object allocations for
 * performance reasons. <b>It is not allowed to retain a reference to the handle* method parameter command instance
 * after the method returns.</b> If you need to pass an instance of the command instance to another thread or retain a
 * reference to it after the handle* method completes, you must make a deep copy of the command instance.
 */
public abstract class PulsarDecoder extends ChannelInboundHandlerAdapter {

    // From the proxy protocol. If present, it means the client is connected via a reverse proxy.
    // The broker can get the real client address and proxy address from the proxy message.
    protected HAProxyMessage proxyMessage;

    private final BaseCommand cmd = new BaseCommand();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage proxyMessage = (HAProxyMessage) msg;
            this.proxyMessage = proxyMessage;
            proxyMessage.release();
            return;
        }
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;
        try {
            // De-serialize the command
            int cmdSize = (int) buffer.readUnsignedInt();
            cmd.parseFrom(buffer, cmdSize);

            if (log.isDebugEnabled()) {
                log.debug("[{}] Received cmd {}", ctx.channel(), cmd.getType());
            }
            messageReceived();

            switch (cmd.getType()) {
            case PARTITIONED_METADATA:
                checkArgument(cmd.hasPartitionMetadata());
                try {
                    interceptCommand(cmd);
                    handlePartitionMetadataRequest(cmd.getPartitionMetadata());
                } catch (InterceptException e) {
                    writeAndFlush(ctx,
                            Commands.newPartitionMetadataResponse(getServerError(e.getErrorCode()),
                            e.getMessage(), cmd.getPartitionMetadata().getRequestId()));
                }
                break;

            case PARTITIONED_METADATA_RESPONSE:
                checkArgument(cmd.hasPartitionMetadataResponse());
                handlePartitionResponse(cmd.getPartitionMetadataResponse());
                break;

            case LOOKUP:
                checkArgument(cmd.hasLookupTopic());
                handleLookup(cmd.getLookupTopic());
                break;

            case LOOKUP_RESPONSE:
                checkArgument(cmd.hasLookupTopicResponse());
                handleLookupResponse(cmd.getLookupTopicResponse());
                break;

            case ACK:
                checkArgument(cmd.hasAck());
                safeInterceptCommand(cmd);
                handleAck(cmd.getAck());
                break;

            case ACK_RESPONSE:
                checkArgument(cmd.hasAckResponse());
                handleAckResponse(cmd.getAckResponse());
                break;

            case CLOSE_CONSUMER:
                checkArgument(cmd.hasCloseConsumer());
                safeInterceptCommand(cmd);
                handleCloseConsumer(cmd.getCloseConsumer());
                break;

            case CLOSE_PRODUCER:
                checkArgument(cmd.hasCloseProducer());
                safeInterceptCommand(cmd);
                handleCloseProducer(cmd.getCloseProducer());
                break;

            case CONNECT:
                checkArgument(cmd.hasConnect());
                handleConnect(cmd.getConnect());
                break;

            case CONNECTED:
                checkArgument(cmd.hasConnected());
                handleConnected(cmd.getConnected());
                break;

            case ERROR:
                checkArgument(cmd.hasError());
                handleError(cmd.getError());
                break;

            case FLOW:
                checkArgument(cmd.hasFlow());
                handleFlow(cmd.getFlow());
                break;

            case MESSAGE: {
                checkArgument(cmd.hasMessage());
                handleMessage(cmd.getMessage(), buffer);
                break;
            }
            case PRODUCER:
                checkArgument(cmd.hasProducer());
                try {
                    interceptCommand(cmd);
                    handleProducer(cmd.getProducer());
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newError(cmd.getProducer().getRequestId(),
                            getServerError(e.getErrorCode()), e.getMessage()));
                }
                break;

            case SEND: {
                checkArgument(cmd.hasSend());
                try {
                    interceptCommand(cmd);
                    // Store a buffer marking the content + headers
                    ByteBuf headersAndPayload = buffer.markReaderIndex();
                    handleSend(cmd.getSend(), headersAndPayload);
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newSendError(cmd.getSend().getProducerId(),
                            cmd.getSend().getSequenceId(), getServerError(e.getErrorCode()), e.getMessage()));
                }
                break;
            }
            case SEND_ERROR:
                checkArgument(cmd.hasSendError());
                handleSendError(cmd.getSendError());
                break;

            case SEND_RECEIPT:
                checkArgument(cmd.hasSendReceipt());
                handleSendReceipt(cmd.getSendReceipt());
                break;

            case SUBSCRIBE:
                checkArgument(cmd.hasSubscribe());
                try {
                    interceptCommand(cmd);
                    handleSubscribe(cmd.getSubscribe());
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newError(cmd.getSubscribe().getRequestId(),
                            getServerError(e.getErrorCode()), e.getMessage()));
                }
                break;

            case SUCCESS:
                checkArgument(cmd.hasSuccess());
                handleSuccess(cmd.getSuccess());
                break;

            case PRODUCER_SUCCESS:
                checkArgument(cmd.hasProducerSuccess());
                handleProducerSuccess(cmd.getProducerSuccess());
                break;

            case UNSUBSCRIBE:
                checkArgument(cmd.hasUnsubscribe());
                safeInterceptCommand(cmd);
                handleUnsubscribe(cmd.getUnsubscribe());
                break;

            case SEEK:
                checkArgument(cmd.hasSeek());
                try {
                    interceptCommand(cmd);
                    handleSeek(cmd.getSeek());
                } catch (InterceptException e) {
                    writeAndFlush(ctx,
                            Commands.newError(
                                    cmd.getSeek().getRequestId(),
                                    getServerError(e.getErrorCode()),
                                    e.getMessage()
                            )
                    );
                }
                break;

            case PING:
                checkArgument(cmd.hasPing());
                handlePing(cmd.getPing());
                break;

            case PONG:
                checkArgument(cmd.hasPong());
                handlePong(cmd.getPong());
                break;

            case REDELIVER_UNACKNOWLEDGED_MESSAGES:
                checkArgument(cmd.hasRedeliverUnacknowledgedMessages());
                handleRedeliverUnacknowledged(cmd.getRedeliverUnacknowledgedMessages());
                break;

            case CONSUMER_STATS:
                checkArgument(cmd.hasConsumerStats());
                handleConsumerStats(cmd.getConsumerStats());
                break;

            case CONSUMER_STATS_RESPONSE:
                checkArgument(cmd.hasConsumerStatsResponse());
                handleConsumerStatsResponse(cmd.getConsumerStatsResponse());
                break;

            case REACHED_END_OF_TOPIC:
                checkArgument(cmd.hasReachedEndOfTopic());
                handleReachedEndOfTopic(cmd.getReachedEndOfTopic());
                break;

            case TOPIC_MIGRATED:
                checkArgument(cmd.hasTopicMigrated());
                handleTopicMigrated(cmd.getTopicMigrated());
                break;

            case GET_LAST_MESSAGE_ID:
                checkArgument(cmd.hasGetLastMessageId());
                handleGetLastMessageId(cmd.getGetLastMessageId());
                break;

            case GET_LAST_MESSAGE_ID_RESPONSE:
                checkArgument(cmd.hasGetLastMessageIdResponse());
                handleGetLastMessageIdSuccess(cmd.getGetLastMessageIdResponse());
                break;

            case ACTIVE_CONSUMER_CHANGE:
                handleActiveConsumerChange(cmd.getActiveConsumerChange());
                break;

            case GET_TOPICS_OF_NAMESPACE:
                checkArgument(cmd.hasGetTopicsOfNamespace());
                try {
                    interceptCommand(cmd);
                    handleGetTopicsOfNamespace(cmd.getGetTopicsOfNamespace());
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newError(cmd.getGetTopicsOfNamespace().getRequestId(),
                            getServerError(e.getErrorCode()), e.getMessage()));
                }
                break;

            case GET_TOPICS_OF_NAMESPACE_RESPONSE:
                checkArgument(cmd.hasGetTopicsOfNamespaceResponse());
                handleGetTopicsOfNamespaceSuccess(cmd.getGetTopicsOfNamespaceResponse());
                break;

            case GET_SCHEMA:
                checkArgument(cmd.hasGetSchema());
                try {
                    interceptCommand(cmd);
                    handleGetSchema(cmd.getGetSchema());
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newGetSchemaResponseError(cmd.getGetSchema().getRequestId(),
                            getServerError(e.getErrorCode()), e.getMessage()));
                }
                break;

            case GET_SCHEMA_RESPONSE:
                checkArgument(cmd.hasGetSchemaResponse());
                handleGetSchemaResponse(cmd.getGetSchemaResponse());
                break;

            case GET_OR_CREATE_SCHEMA:
                checkArgument(cmd.hasGetOrCreateSchema());
                try {
                    interceptCommand(cmd);
                    handleGetOrCreateSchema(cmd.getGetOrCreateSchema());
                } catch (InterceptException e) {
                    writeAndFlush(ctx, Commands.newGetOrCreateSchemaResponseError(
                            cmd.getGetOrCreateSchema().getRequestId(), getServerError(e.getErrorCode()),
                            e.getMessage()));
                }
                break;

            case GET_OR_CREATE_SCHEMA_RESPONSE:
                checkArgument(cmd.hasGetOrCreateSchemaResponse());
                handleGetOrCreateSchemaResponse(cmd.getGetOrCreateSchemaResponse());
                break;

            case AUTH_CHALLENGE:
                checkArgument(cmd.hasAuthChallenge());
                handleAuthChallenge(cmd.getAuthChallenge());
                break;

            case AUTH_RESPONSE:
                checkArgument(cmd.hasAuthResponse());
                handleAuthResponse(cmd.getAuthResponse());
                break;

            case TC_CLIENT_CONNECT_REQUEST:
                checkArgument(cmd.hasTcClientConnectRequest());
                handleTcClientConnectRequest(cmd.getTcClientConnectRequest());
                break;

            case TC_CLIENT_CONNECT_RESPONSE:
                checkArgument(cmd.hasTcClientConnectResponse());
                handleTcClientConnectResponse(cmd.getTcClientConnectResponse());
                break;

            case NEW_TXN:
                checkArgument(cmd.hasNewTxn());
                handleNewTxn(cmd.getNewTxn());
                break;

            case NEW_TXN_RESPONSE:
                checkArgument(cmd.hasNewTxnResponse());
                handleNewTxnResponse(cmd.getNewTxnResponse());
                break;

            case ADD_PARTITION_TO_TXN:
                checkArgument(cmd.hasAddPartitionToTxn());
                handleAddPartitionToTxn(cmd.getAddPartitionToTxn());
                break;

            case ADD_PARTITION_TO_TXN_RESPONSE:
                checkArgument(cmd.hasAddPartitionToTxnResponse());
                handleAddPartitionToTxnResponse(cmd.getAddPartitionToTxnResponse());
                break;

            case ADD_SUBSCRIPTION_TO_TXN:
                checkArgument(cmd.hasAddSubscriptionToTxn());
                handleAddSubscriptionToTxn(cmd.getAddSubscriptionToTxn());
                break;

            case ADD_SUBSCRIPTION_TO_TXN_RESPONSE:
                checkArgument(cmd.hasAddSubscriptionToTxnResponse());
                handleAddSubscriptionToTxnResponse(cmd.getAddSubscriptionToTxnResponse());
                break;

            case END_TXN:
                checkArgument(cmd.hasEndTxn());
                handleEndTxn(cmd.getEndTxn());
                break;

            case END_TXN_RESPONSE:
                checkArgument(cmd.hasEndTxnResponse());
                handleEndTxnResponse(cmd.getEndTxnResponse());
                break;

            case END_TXN_ON_PARTITION:
                checkArgument(cmd.hasEndTxnOnPartition());
                handleEndTxnOnPartition(cmd.getEndTxnOnPartition());
                break;

            case END_TXN_ON_PARTITION_RESPONSE:
                checkArgument(cmd.hasEndTxnOnPartitionResponse());
                handleEndTxnOnPartitionResponse(cmd.getEndTxnOnPartitionResponse());
                break;

            case END_TXN_ON_SUBSCRIPTION:
                checkArgument(cmd.hasEndTxnOnSubscription());
                handleEndTxnOnSubscription(cmd.getEndTxnOnSubscription());
                break;

            case END_TXN_ON_SUBSCRIPTION_RESPONSE:
                checkArgument(cmd.hasEndTxnOnSubscriptionResponse());
                handleEndTxnOnSubscriptionResponse(cmd.getEndTxnOnSubscriptionResponse());
                break;

            case WATCH_TOPIC_LIST:
                checkArgument(cmd.hasWatchTopicList());
                handleCommandWatchTopicList(cmd.getWatchTopicList());
                break;

            case WATCH_TOPIC_LIST_SUCCESS:
                checkArgument(cmd.hasWatchTopicListSuccess());
                handleCommandWatchTopicListSuccess(cmd.getWatchTopicListSuccess());
                break;

            case WATCH_TOPIC_UPDATE:
                checkArgument(cmd.hasWatchTopicUpdate());
                handleCommandWatchTopicUpdate(cmd.getWatchTopicUpdate());
                break;

            case WATCH_TOPIC_LIST_CLOSE:
                checkArgument(cmd.hasWatchTopicListClose());
                handleCommandWatchTopicListClose(cmd.getWatchTopicListClose());
                break;

            default:
                break;
            }
        } finally {
            buffer.release();
            // Clear the fields in cmd to release memory.
            // The clear() call below also helps prevent misuse of holding references to command objects after
            // handle* methods complete, as per the class javadoc requirement.
            // While this doesn't completely prevent such misuse, it makes tests more likely to catch violations.
            cmd.clear();
        }
    }

    protected abstract void messageReceived();

    private ServerError getServerError(int errorCode) {
        ServerError serverError = ServerError.valueOf(errorCode);
        return serverError == null ? ServerError.UnknownError : serverError;
    }

    private void safeInterceptCommand(BaseCommand command) {
        try {
            interceptCommand(command);
        } catch (InterceptException e) {
            // no-op
        }
    }

    protected void interceptCommand(BaseCommand command) throws InterceptException {
        //No-op
    }

    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata response) {
        throw new UnsupportedOperationException();
    }

    protected void handlePartitionResponse(CommandPartitionedTopicMetadataResponse response) {
        throw new UnsupportedOperationException();
    }

    protected void handleLookup(CommandLookupTopic lookup) {
        throw new UnsupportedOperationException();
    }

    protected void handleLookupResponse(CommandLookupTopicResponse connection) {
        throw new UnsupportedOperationException();
    }

    protected void handleConnect(CommandConnect connect) {
        throw new UnsupportedOperationException();
    }

    protected void handleConnected(CommandConnected connected) {
        throw new UnsupportedOperationException();
    }

    protected void handleSubscribe(CommandSubscribe subscribe) {
        throw new UnsupportedOperationException();
    }

    protected void handleProducer(CommandProducer producer) {
        throw new UnsupportedOperationException();
    }

    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        throw new UnsupportedOperationException();
    }

    protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
        throw new UnsupportedOperationException();
    }

    protected void handleSendError(CommandSendError sendError) {
        throw new UnsupportedOperationException();
    }

    protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
        throw new UnsupportedOperationException();
    }

    protected void handleAck(CommandAck ack) {
        throw new UnsupportedOperationException();
    }

    protected void handleAckResponse(CommandAckResponse ackResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleFlow(CommandFlow flow) {
        throw new UnsupportedOperationException();
    }

    protected void handleRedeliverUnacknowledged(CommandRedeliverUnacknowledgedMessages redeliver) {
        throw new UnsupportedOperationException();
    }

    protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
        throw new UnsupportedOperationException();
    }

    protected void handleSeek(CommandSeek seek) {
        throw new UnsupportedOperationException();
    }

    protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
        throw new UnsupportedOperationException();
    }

    protected void handleSuccess(CommandSuccess success) {
        throw new UnsupportedOperationException();
    }

    protected void handleProducerSuccess(CommandProducerSuccess success) {
        throw new UnsupportedOperationException();
    }

    protected void handleError(CommandError error) {
        throw new UnsupportedOperationException();
    }

    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        throw new UnsupportedOperationException();
    }

    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        throw new UnsupportedOperationException();
    }

    protected void handlePing(CommandPing ping) {
        throw new UnsupportedOperationException();
    }

    protected void handlePong(CommandPong pong) {
        throw new UnsupportedOperationException();
    }

    protected void handleConsumerStats(CommandConsumerStats commandConsumerStats) {
        throw new UnsupportedOperationException();
    }

    protected void handleConsumerStatsResponse(CommandConsumerStatsResponse commandConsumerStatsResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleReachedEndOfTopic(CommandReachedEndOfTopic commandReachedEndOfTopic) {
        throw new UnsupportedOperationException();
    }

    protected void handleTopicMigrated(CommandTopicMigrated commandMigratedTopic) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetLastMessageId(CommandGetLastMessageId getLastMessageId) {
        throw new UnsupportedOperationException();
    }
    protected void handleGetLastMessageIdSuccess(CommandGetLastMessageIdResponse success) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetTopicsOfNamespaceSuccess(CommandGetTopicsOfNamespaceResponse response) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetSchema(CommandGetSchema commandGetSchema) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetOrCreateSchema(CommandGetOrCreateSchema commandGetOrCreateSchema) {
        throw new UnsupportedOperationException();
    }

    protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse commandGetOrCreateSchemaResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleAuthResponse(CommandAuthResponse commandAuthResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleAuthChallenge(CommandAuthChallenge commandAuthChallenge) {
        throw new UnsupportedOperationException();
    }

    protected void handleTcClientConnectRequest(CommandTcClientConnectRequest tcClientConnectRequest) {
        throw new UnsupportedOperationException();
    }

    protected void handleTcClientConnectResponse(CommandTcClientConnectResponse tcClientConnectResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleNewTxn(CommandNewTxn commandNewTxn) {
        throw new UnsupportedOperationException();
    }

    protected void handleNewTxnResponse(CommandNewTxnResponse commandNewTxnResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleAddPartitionToTxn(CommandAddPartitionToTxn commandAddPartitionToTxn) {
        throw new UnsupportedOperationException();
    }

    protected void handleAddPartitionToTxnResponse(CommandAddPartitionToTxnResponse commandAddPartitionToTxnResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleAddSubscriptionToTxn(CommandAddSubscriptionToTxn commandAddSubscriptionToTxn) {
        throw new UnsupportedOperationException();
    }

    protected void handleAddSubscriptionToTxnResponse(
        CommandAddSubscriptionToTxnResponse commandAddSubscriptionToTxnResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxn(CommandEndTxn commandEndTxn) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxnResponse(CommandEndTxnResponse commandEndTxnResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxnOnPartition(CommandEndTxnOnPartition commandEndTxnOnPartition) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxnOnPartitionResponse(CommandEndTxnOnPartitionResponse commandEndTxnOnPartitionResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxnOnSubscription(CommandEndTxnOnSubscription commandEndTxnOnSubscription) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndTxnOnSubscriptionResponse(
        CommandEndTxnOnSubscriptionResponse commandEndTxnOnSubscriptionResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleCommandWatchTopicList(CommandWatchTopicList commandWatchTopicList) {
        throw new UnsupportedOperationException();
    }

    protected void handleCommandWatchTopicListSuccess(
            CommandWatchTopicListSuccess commandWatchTopicListSuccess) {
        throw new UnsupportedOperationException();
    }

    protected void handleCommandWatchTopicUpdate(CommandWatchTopicUpdate commandWatchTopicUpdate) {
        throw new UnsupportedOperationException();
    }

    protected void handleCommandWatchTopicListClose(CommandWatchTopicListClose commandWatchTopicListClose) {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarDecoder.class);

    private void writeAndFlush(ChannelOutboundInvoker ctx, ByteBuf cmd) {
        NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, cmd);
    }
}
