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
package org.apache.pulsar.common.protocol;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString.copyFrom;
import static org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString.copyFromUtf8;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.AuthMethod;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.ValidationError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAckResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAddPartitionToTxn;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAddSubscriptionToTxn;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnected;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConsumerStatsResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxn;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxnOnPartition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxnOnSubscription;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandFlow;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetLastMessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandMessage;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandNewTxn;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPing;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPong;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandReachedEndOfTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSeek;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSend;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.FeatureFlags;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyLongValue;
import org.apache.pulsar.common.api.proto.PulsarApi.KeySharedMeta;
import org.apache.pulsar.common.api.proto.PulsarApi.KeySharedMode;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.Schema;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnAction;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;

@UtilityClass
@Slf4j
@SuppressWarnings("checkstyle:JavadocType")
public class Commands {

    // default message size for transfer
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 5 * 1024 * 1024;
    public static final int MESSAGE_SIZE_FRAME_PADDING = 10 * 1024;
    public static final int INVALID_MAX_MESSAGE_SIZE = -1;

    @SuppressWarnings("checkstyle:ConstantName")
    public static final short magicCrc32c = 0x0e01;
    private static final int checksumSize = 4;

    public static ByteBuf newConnect(String authMethodName, String authData, String libVersion) {
        return newConnect(authMethodName, authData, getCurrentProtocolVersion(), libVersion, null /* target broker */,
                null /* originalPrincipal */, null /* Client Auth Data */, null /* Client Auth Method */);
    }

    public static ByteBuf newConnect(String authMethodName, String authData, String libVersion, String targetBroker) {
        return newConnect(authMethodName, authData, getCurrentProtocolVersion(), libVersion, targetBroker, null, null,
            null);
    }

    public static ByteBuf newConnect(String authMethodName, String authData, String libVersion, String targetBroker,
            String originalPrincipal, String clientAuthData, String clientAuthMethod) {
        return newConnect(authMethodName, authData, getCurrentProtocolVersion(), libVersion, targetBroker,
                originalPrincipal, clientAuthData, clientAuthMethod);
    }

    public static FeatureFlags getFeatureFlags() {
        FeatureFlags.Builder flags = FeatureFlags.newBuilder();
        flags.setSupportsAuthRefresh(true);
        return flags.build();
    }

    public static ByteBuf newConnect(String authMethodName, String authData, int protocolVersion, String libVersion,
            String targetBroker, String originalPrincipal, String originalAuthData,
            String originalAuthMethod) {
        CommandConnect.Builder connectBuilder = CommandConnect.newBuilder();
        connectBuilder.setClientVersion(libVersion != null ? libVersion : "Pulsar Client");
        connectBuilder.setAuthMethodName(authMethodName);

        if ("ycav1".equals(authMethodName)) {
            // Handle the case of a client that gets updated before the broker and starts sending the string auth method
            // name. An example would be in broker-to-broker replication. We need to make sure the clients are still
            // passing both the enum and the string until all brokers are upgraded.
            connectBuilder.setAuthMethod(AuthMethod.AuthMethodYcaV1);
        }

        if (targetBroker != null) {
            // When connecting through a proxy, we need to specify which broker do we want to be proxied through
            connectBuilder.setProxyToBrokerUrl(targetBroker);
        }

        if (authData != null) {
            connectBuilder.setAuthData(copyFromUtf8(authData));
        }

        if (originalPrincipal != null) {
            connectBuilder.setOriginalPrincipal(originalPrincipal);
        }

        if (originalAuthData != null) {
            connectBuilder.setOriginalAuthData(originalAuthData);
        }

        if (originalAuthMethod != null) {
            connectBuilder.setOriginalAuthMethod(originalAuthMethod);
        }
        connectBuilder.setProtocolVersion(protocolVersion);

        connectBuilder.setFeatureFlags(getFeatureFlags());
        CommandConnect connect = connectBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.CONNECT).setConnect(connect));
        connect.recycle();
        connectBuilder.recycle();
        return res;
    }

    public static ByteBuf newConnect(String authMethodName, AuthData authData, int protocolVersion, String libVersion,
                                     String targetBroker, String originalPrincipal, AuthData originalAuthData,
                                     String originalAuthMethod) {
        CommandConnect.Builder connectBuilder = CommandConnect.newBuilder();
        connectBuilder.setClientVersion(libVersion != null ? libVersion : "Pulsar Client");
        connectBuilder.setAuthMethodName(authMethodName);

        if (targetBroker != null) {
            // When connecting through a proxy, we need to specify which broker do we want to be proxied through
            connectBuilder.setProxyToBrokerUrl(targetBroker);
        }

        if (authData != null) {
            connectBuilder.setAuthData(ByteString.copyFrom(authData.getBytes()));
        }

        if (originalPrincipal != null) {
            connectBuilder.setOriginalPrincipal(originalPrincipal);
        }

        if (originalAuthData != null) {
            connectBuilder.setOriginalAuthData(new String(originalAuthData.getBytes(), UTF_8));
        }

        if (originalAuthMethod != null) {
            connectBuilder.setOriginalAuthMethod(originalAuthMethod);
        }
        connectBuilder.setProtocolVersion(protocolVersion);
        connectBuilder.setFeatureFlags(getFeatureFlags());
        CommandConnect connect = connectBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.CONNECT).setConnect(connect));
        connect.recycle();
        connectBuilder.recycle();
        return res;
    }

    public static ByteBuf newConnected(int clientProtocoVersion) {
        return newConnected(clientProtocoVersion, INVALID_MAX_MESSAGE_SIZE);
    }

    public static ByteBuf newConnected(int clientProtocolVersion, int maxMessageSize) {
        CommandConnected.Builder connectedBuilder = CommandConnected.newBuilder();
        connectedBuilder.setServerVersion("Pulsar Server");
        if (INVALID_MAX_MESSAGE_SIZE != maxMessageSize) {
            connectedBuilder.setMaxMessageSize(maxMessageSize);
        }

        // If the broker supports a newer version of the protocol, it will anyway advertise the max version that the
        // client supports, to avoid confusing the client.
        int currentProtocolVersion = getCurrentProtocolVersion();
        int versionToAdvertise = Math.min(currentProtocolVersion, clientProtocolVersion);

        connectedBuilder.setProtocolVersion(versionToAdvertise);

        CommandConnected connected = connectedBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.CONNECTED).setConnected(connected));
        connected.recycle();
        connectedBuilder.recycle();
        return res;
    }

    public static ByteBuf newAuthChallenge(String authMethod, AuthData brokerData, int clientProtocolVersion) {
        CommandAuthChallenge.Builder challengeBuilder = CommandAuthChallenge.newBuilder();

        // If the broker supports a newer version of the protocol, it will anyway advertise the max version that the
        // client supports, to avoid confusing the client.
        int currentProtocolVersion = getCurrentProtocolVersion();
        int versionToAdvertise = Math.min(currentProtocolVersion, clientProtocolVersion);

        challengeBuilder.setProtocolVersion(versionToAdvertise);

        byte[] authData = brokerData != null ? brokerData.getBytes() : new byte[0];

        CommandAuthChallenge challenge = challengeBuilder
            .setChallenge(PulsarApi.AuthData.newBuilder()
                .setAuthData(copyFrom(authData))
                .setAuthMethodName(authMethod)
                .build())
            .build();

        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.AUTH_CHALLENGE).setAuthChallenge(challenge));
        challenge.recycle();
        challengeBuilder.recycle();
        return res;
    }

    public static ByteBuf newAuthResponse(String authMethod,
                                           AuthData clientData,
                                           int clientProtocolVersion,
                                           String clientVersion) {
        CommandAuthResponse.Builder responseBuilder  = CommandAuthResponse.newBuilder();

        responseBuilder.setClientVersion(clientVersion != null ? clientVersion : "Pulsar Client");
        responseBuilder.setProtocolVersion(clientProtocolVersion);

        CommandAuthResponse response = responseBuilder
            .setResponse(PulsarApi.AuthData.newBuilder()
                .setAuthData(copyFrom(clientData.getBytes()))
                .setAuthMethodName(authMethod)
                .build())
            .build();

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.AUTH_RESPONSE).setAuthResponse(response));
        response.recycle();
        responseBuilder.recycle();
        return res;
    }

    public static ByteBuf newSuccess(long requestId) {
        CommandSuccess.Builder successBuilder = CommandSuccess.newBuilder();
        successBuilder.setRequestId(requestId);
        CommandSuccess success = successBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.SUCCESS).setSuccess(success));
        successBuilder.recycle();
        success.recycle();
        return res;
    }

    public static ByteBuf newProducerSuccess(long requestId, String producerName, SchemaVersion schemaVersion) {
        return newProducerSuccess(requestId, producerName, -1, schemaVersion);
    }

    public static ByteBuf newProducerSuccess(long requestId, String producerName, long lastSequenceId,
        SchemaVersion schemaVersion) {
        CommandProducerSuccess.Builder producerSuccessBuilder = CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setRequestId(requestId);
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        CommandProducerSuccess producerSuccess = producerSuccessBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.PRODUCER_SUCCESS).setProducerSuccess(producerSuccess));
        producerSuccess.recycle();
        producerSuccessBuilder.recycle();
        return res;
    }

    public static ByteBuf newError(long requestId, ServerError error, String message) {
        CommandError.Builder cmdErrorBuilder = CommandError.newBuilder();
        cmdErrorBuilder.setRequestId(requestId);
        cmdErrorBuilder.setError(error);
        cmdErrorBuilder.setMessage(message);
        CommandError cmdError = cmdErrorBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ERROR).setError(cmdError));
        cmdError.recycle();
        cmdErrorBuilder.recycle();
        return res;

    }

    public static ByteBuf newSendReceipt(long producerId, long sequenceId, long highestId, long ledgerId,
             long entryId) {
        CommandSendReceipt.Builder sendReceiptBuilder = CommandSendReceipt.newBuilder();
        sendReceiptBuilder.setProducerId(producerId);
        sendReceiptBuilder.setSequenceId(sequenceId);
        sendReceiptBuilder.setHighestSequenceId(highestId);
        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        MessageIdData messageId = messageIdBuilder.build();
        sendReceiptBuilder.setMessageId(messageId);
        CommandSendReceipt sendReceipt = sendReceiptBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.SEND_RECEIPT).setSendReceipt(sendReceipt));
        messageIdBuilder.recycle();
        messageId.recycle();
        sendReceiptBuilder.recycle();
        sendReceipt.recycle();
        return res;
    }

    public static ByteBuf newSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
        CommandSendError.Builder sendErrorBuilder = CommandSendError.newBuilder();
        sendErrorBuilder.setProducerId(producerId);
        sendErrorBuilder.setSequenceId(sequenceId);
        sendErrorBuilder.setError(error);
        sendErrorBuilder.setMessage(errorMsg);
        CommandSendError sendError = sendErrorBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.SEND_ERROR).setSendError(sendError));
        sendErrorBuilder.recycle();
        sendError.recycle();
        return res;
    }


    public static boolean hasChecksum(ByteBuf buffer) {
        return buffer.getShort(buffer.readerIndex()) == magicCrc32c;
    }

    /**
     * Read the checksum and advance the reader index in the buffer.
     *
     * <p>Note: This method assume the checksum presence was already verified before.
     */
    public static int readChecksum(ByteBuf buffer) {
        buffer.skipBytes(2); //skip magic bytes
        return buffer.readInt();
    }

    public static void skipChecksumIfPresent(ByteBuf buffer) {
        if (hasChecksum(buffer)) {
            readChecksum(buffer);
        }
    }

    public static MessageMetadata parseMessageMetadata(ByteBuf buffer) {
        try {
            // initially reader-index may point to start_of_checksum : increment reader-index to start_of_metadata
            // to parse metadata
            skipChecksumIfPresent(buffer);
            int metadataSize = (int) buffer.readUnsignedInt();

            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + metadataSize);
            ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
            MessageMetadata.Builder messageMetadataBuilder = MessageMetadata.newBuilder();
            MessageMetadata res = messageMetadataBuilder.mergeFrom(stream, null).build();
            buffer.writerIndex(writerIndex);
            messageMetadataBuilder.recycle();
            stream.recycle();
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void skipMessageMetadata(ByteBuf buffer) {
        // initially reader-index may point to start_of_checksum : increment reader-index to start_of_metadata to parse
        // metadata
        skipChecksumIfPresent(buffer);
        int metadataSize = (int) buffer.readUnsignedInt();
        buffer.skipBytes(metadataSize);
    }

    public static ByteBufPair newMessage(long consumerId, MessageIdData messageId, int redeliveryCount,
        ByteBuf metadataAndPayload, long[] ackSet) {
        CommandMessage.Builder msgBuilder = CommandMessage.newBuilder();
        msgBuilder.setConsumerId(consumerId);
        msgBuilder.setMessageId(messageId);
        if (redeliveryCount > 0) {
            msgBuilder.setRedeliveryCount(redeliveryCount);
        }
        if (ackSet != null) {
            msgBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(ackSet));
        }
        CommandMessage msg = msgBuilder.build();
        BaseCommand.Builder cmdBuilder = BaseCommand.newBuilder();
        BaseCommand cmd = cmdBuilder.setType(Type.MESSAGE).setMessage(msg).build();

        ByteBufPair res = serializeCommandMessageWithSize(cmd, metadataAndPayload);
        cmd.recycle();
        cmdBuilder.recycle();
        msg.recycle();
        msgBuilder.recycle();
        return res;
    }

    public static ByteBufPair newSend(long producerId, long sequenceId, int numMessaegs, ChecksumType checksumType,
                                      MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(producerId, sequenceId, numMessaegs,
                messageMetadata.hasTxnidLeastBits() ? messageMetadata.getTxnidLeastBits() : -1,
                messageMetadata.hasTxnidMostBits() ? messageMetadata.getTxnidMostBits() : -1,
                checksumType, messageMetadata, payload);
    }

    public static ByteBufPair newSend(long producerId, long lowestSequenceId, long highestSequenceId, int numMessaegs,
              ChecksumType checksumType, MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(producerId, lowestSequenceId, highestSequenceId, numMessaegs,
                messageMetadata.hasTxnidLeastBits() ? messageMetadata.getTxnidLeastBits() : -1,
                messageMetadata.hasTxnidMostBits() ? messageMetadata.getTxnidMostBits() : -1,
                checksumType, messageMetadata, payload);
    }

    public static ByteBufPair newSend(long producerId, long sequenceId, int numMessages,
                                      long txnIdLeastBits, long txnIdMostBits, ChecksumType checksumType,
            MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setProducerId(producerId);
        sendBuilder.setSequenceId(sequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (txnIdLeastBits >= 0) {
            sendBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits >= 0) {
            sendBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (messageData.hasTotalChunkMsgSize() && messageData.getTotalChunkMsgSize() > 1) {
            sendBuilder.setIsChunk(true);
        }
        CommandSend send = sendBuilder.build();

        ByteBufPair res = serializeCommandSendWithSize(BaseCommand.newBuilder().setType(Type.SEND).setSend(send),
                checksumType, messageData, payload);
        send.recycle();
        sendBuilder.recycle();
        return res;
    }

    public static ByteBufPair newSend(long producerId, long lowestSequenceId, long highestSequenceId, int numMessages,
          long txnIdLeastBits, long txnIdMostBits, ChecksumType checksumType,
          MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setProducerId(producerId);
        sendBuilder.setSequenceId(lowestSequenceId);
        sendBuilder.setHighestSequenceId(highestSequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (txnIdLeastBits >= 0) {
            sendBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits >= 0) {
            sendBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (messageData.hasTotalChunkMsgSize() && messageData.getTotalChunkMsgSize() > 1) {
            sendBuilder.setIsChunk(true);
        }
        CommandSend send = sendBuilder.build();

        ByteBufPair res = serializeCommandSendWithSize(BaseCommand.newBuilder().setType(Type.SEND).setSend(send),
                checksumType, messageData, payload);
        send.recycle();
        sendBuilder.recycle();
        return res;
    }

    public static ByteBuf newSubscribe(String topic, String subscription, long consumerId, long requestId,
            SubType subType, int priorityLevel, String consumerName, long resetStartMessageBackInSeconds) {
        return newSubscribe(topic, subscription, consumerId, requestId, subType, priorityLevel, consumerName,
                true /* isDurable */, null /* startMessageId */, Collections.emptyMap(), false,
                false /* isReplicated */, InitialPosition.Earliest, resetStartMessageBackInSeconds, null,
                true /* createTopicIfDoesNotExist */);
    }

    public static ByteBuf newSubscribe(String topic, String subscription, long consumerId, long requestId,
            SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageIdData startMessageId,
            Map<String, String> metadata, boolean readCompacted, boolean isReplicated,
            InitialPosition subscriptionInitialPosition, long startMessageRollbackDurationInSec, SchemaInfo schemaInfo,
            boolean createTopicIfDoesNotExist) {
                return newSubscribe(topic, subscription, consumerId, requestId, subType, priorityLevel, consumerName,
                        isDurable, startMessageId, metadata, readCompacted, isReplicated, subscriptionInitialPosition,
                        startMessageRollbackDurationInSec, schemaInfo, createTopicIfDoesNotExist, null);
    }

    public static ByteBuf newSubscribe(String topic, String subscription, long consumerId, long requestId,
               SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageIdData startMessageId,
               Map<String, String> metadata, boolean readCompacted, boolean isReplicated,
               InitialPosition subscriptionInitialPosition, long startMessageRollbackDurationInSec,
               SchemaInfo schemaInfo, boolean createTopicIfDoesNotExist, KeySharedPolicy keySharedPolicy) {
        CommandSubscribe.Builder subscribeBuilder = CommandSubscribe.newBuilder();
        subscribeBuilder.setTopic(topic);
        subscribeBuilder.setSubscription(subscription);
        subscribeBuilder.setSubType(subType);
        subscribeBuilder.setConsumerId(consumerId);
        subscribeBuilder.setConsumerName(consumerName);
        subscribeBuilder.setRequestId(requestId);
        subscribeBuilder.setPriorityLevel(priorityLevel);
        subscribeBuilder.setDurable(isDurable);
        subscribeBuilder.setReadCompacted(readCompacted);
        subscribeBuilder.setInitialPosition(subscriptionInitialPosition);
        subscribeBuilder.setReplicateSubscriptionState(isReplicated);
        subscribeBuilder.setForceTopicCreation(createTopicIfDoesNotExist);

        if (keySharedPolicy != null) {
            KeySharedMeta.Builder keySharedMetaBuilder = PulsarApi.KeySharedMeta.newBuilder();
            keySharedMetaBuilder.setAllowOutOfOrderDelivery(keySharedPolicy.isAllowOutOfOrderDelivery());
            keySharedMetaBuilder.setKeySharedMode(convertKeySharedMode(keySharedPolicy.getKeySharedMode()));

            if (keySharedPolicy instanceof KeySharedPolicy.KeySharedPolicySticky) {
                List<Range> ranges = ((KeySharedPolicy.KeySharedPolicySticky) keySharedPolicy)
                        .getRanges();
                for (Range range : ranges) {
                    keySharedMetaBuilder.addHashRanges(PulsarApi.IntRange.newBuilder()
                            .setStart(range.getStart())
                            .setEnd(range.getEnd()));
                }
            }

            subscribeBuilder.setKeySharedMeta(keySharedMetaBuilder.build());
        }

        if (startMessageId != null) {
            subscribeBuilder.setStartMessageId(startMessageId);
        }
        if (startMessageRollbackDurationInSec > 0) {
            subscribeBuilder.setStartMessageRollbackDurationSec(startMessageRollbackDurationInSec);
        }
        subscribeBuilder.addAllMetadata(CommandUtils.toKeyValueList(metadata));

        Schema schema = null;
        if (schemaInfo != null) {
            schema = getSchema(schemaInfo);
            subscribeBuilder.setSchema(schema);
        }

        CommandSubscribe subscribe = subscribeBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.SUBSCRIBE).setSubscribe(subscribe));
        subscribeBuilder.recycle();
        subscribe.recycle();
        if (null != schema) {
            schema.recycle();
        }
        return res;
    }


    private static KeySharedMode convertKeySharedMode(org.apache.pulsar.client.api.KeySharedMode mode) {
        switch (mode) {
        case AUTO_SPLIT: return KeySharedMode.AUTO_SPLIT;
        case STICKY: return KeySharedMode.STICKY;
        default:
            throw new IllegalArgumentException("Unexpected key shared mode: " + mode);
        }
    }

    public static ByteBuf newUnsubscribe(long consumerId, long requestId) {
        CommandUnsubscribe.Builder unsubscribeBuilder = CommandUnsubscribe.newBuilder();
        unsubscribeBuilder.setConsumerId(consumerId);
        unsubscribeBuilder.setRequestId(requestId);
        CommandUnsubscribe unsubscribe = unsubscribeBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.UNSUBSCRIBE).setUnsubscribe(unsubscribe));
        unsubscribeBuilder.recycle();
        unsubscribe.recycle();
        return res;
    }

    public static ByteBuf newActiveConsumerChange(long consumerId, boolean isActive) {
        CommandActiveConsumerChange.Builder changeBuilder = CommandActiveConsumerChange.newBuilder()
            .setConsumerId(consumerId)
            .setIsActive(isActive);

        CommandActiveConsumerChange change = changeBuilder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.ACTIVE_CONSUMER_CHANGE).setActiveConsumerChange(change));
        changeBuilder.recycle();
        change.recycle();
        return res;
    }

    public static ByteBuf newSeek(long consumerId, long requestId, long ledgerId, long entryId) {
        CommandSeek.Builder seekBuilder = CommandSeek.newBuilder();
        seekBuilder.setConsumerId(consumerId);
        seekBuilder.setRequestId(requestId);

        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        MessageIdData messageId = messageIdBuilder.build();
        seekBuilder.setMessageId(messageId);

        CommandSeek seek = seekBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.SEEK).setSeek(seek));
        messageId.recycle();
        messageIdBuilder.recycle();
        seekBuilder.recycle();
        seek.recycle();
        return res;
    }

    public static ByteBuf newSeek(long consumerId, long requestId, long timestamp) {
        CommandSeek.Builder seekBuilder = CommandSeek.newBuilder();
        seekBuilder.setConsumerId(consumerId);
        seekBuilder.setRequestId(requestId);

        seekBuilder.setMessagePublishTime(timestamp);

        CommandSeek seek = seekBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.SEEK).setSeek(seek));

        seekBuilder.recycle();
        seek.recycle();
        return res;
    }

    public static ByteBuf newCloseConsumer(long consumerId, long requestId) {
        CommandCloseConsumer.Builder closeConsumerBuilder = CommandCloseConsumer.newBuilder();
        closeConsumerBuilder.setConsumerId(consumerId);
        closeConsumerBuilder.setRequestId(requestId);
        CommandCloseConsumer closeConsumer = closeConsumerBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.CLOSE_CONSUMER).setCloseConsumer(closeConsumer));
        closeConsumerBuilder.recycle();
        closeConsumer.recycle();
        return res;
    }

    public static ByteBuf newReachedEndOfTopic(long consumerId) {
        CommandReachedEndOfTopic.Builder reachedEndOfTopicBuilder = CommandReachedEndOfTopic.newBuilder();
        reachedEndOfTopicBuilder.setConsumerId(consumerId);
        CommandReachedEndOfTopic reachedEndOfTopic = reachedEndOfTopicBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.REACHED_END_OF_TOPIC).setReachedEndOfTopic(reachedEndOfTopic));
        reachedEndOfTopicBuilder.recycle();
        reachedEndOfTopic.recycle();
        return res;
    }

    public static ByteBuf newCloseProducer(long producerId, long requestId) {
        CommandCloseProducer.Builder closeProducerBuilder = CommandCloseProducer.newBuilder();
        closeProducerBuilder.setProducerId(producerId);
        closeProducerBuilder.setRequestId(requestId);
        CommandCloseProducer closeProducer = closeProducerBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.CLOSE_PRODUCER).setCloseProducer(closeProducerBuilder));
        closeProducerBuilder.recycle();
        closeProducer.recycle();
        return res;
    }

    @VisibleForTesting
    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                Map<String, String> metadata) {
        return newProducer(topic, producerId, requestId, producerName, false, metadata);
    }

    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                boolean encrypted, Map<String, String> metadata) {
        return newProducer(topic, producerId, requestId, producerName, encrypted, metadata, null, 0, false);
    }

    private static Schema.Type getSchemaType(SchemaType type) {
        if (type.getValue() < 0) {
            return Schema.Type.None;
        } else {
            return Schema.Type.valueOf(type.getValue());
        }
    }

    public static SchemaType getSchemaType(Schema.Type type) {
        if (type.getNumber() < 0) {
            // this is unexpected
            return SchemaType.NONE;
        } else {
            return SchemaType.valueOf(type.getNumber());
        }
    }

    private static Schema getSchema(SchemaInfo schemaInfo) {
        Schema.Builder builder = Schema.newBuilder()
            .setName(schemaInfo.getName())
            .setSchemaData(copyFrom(schemaInfo.getSchema()))
            .setType(getSchemaType(schemaInfo.getType()))
            .addAllProperties(
                schemaInfo.getProperties().entrySet().stream().map(entry ->
                    KeyValue.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                        .build()
                ).collect(Collectors.toList())
            );
        Schema schema = builder.build();
        builder.recycle();
        return schema;
    }

    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
          boolean encrypted, Map<String, String> metadata, SchemaInfo schemaInfo,
          long epoch, boolean userProvidedProducerName) {
        CommandProducer.Builder producerBuilder = CommandProducer.newBuilder();
        producerBuilder.setTopic(topic);
        producerBuilder.setProducerId(producerId);
        producerBuilder.setRequestId(requestId);
        producerBuilder.setEpoch(epoch);
        if (producerName != null) {
            producerBuilder.setProducerName(producerName);
        }
        producerBuilder.setUserProvidedProducerName(userProvidedProducerName);
        producerBuilder.setEncrypted(encrypted);

        producerBuilder.addAllMetadata(CommandUtils.toKeyValueList(metadata));

        if (null != schemaInfo) {
            producerBuilder.setSchema(getSchema(schemaInfo));
        }

        CommandProducer producer = producerBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.PRODUCER).setProducer(producer));
        producerBuilder.recycle();
        producer.recycle();
        return res;
    }

    public static ByteBuf newPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {
        CommandPartitionedTopicMetadataResponse.Builder partitionMetadataResponseBuilder =
            CommandPartitionedTopicMetadataResponse.newBuilder();
        partitionMetadataResponseBuilder.setRequestId(requestId);
        partitionMetadataResponseBuilder.setError(error);
        partitionMetadataResponseBuilder.setResponse(CommandPartitionedTopicMetadataResponse.LookupType.Failed);
        if (errorMsg != null) {
            partitionMetadataResponseBuilder.setMessage(errorMsg);
        }

        CommandPartitionedTopicMetadataResponse partitionMetadataResponse = partitionMetadataResponseBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.PARTITIONED_METADATA_RESPONSE)
                .setPartitionMetadataResponse(partitionMetadataResponse));
        partitionMetadataResponseBuilder.recycle();
        partitionMetadataResponse.recycle();
        return res;
    }

    public static ByteBuf newPartitionMetadataRequest(String topic, long requestId) {
        CommandPartitionedTopicMetadata.Builder partitionMetadataBuilder = CommandPartitionedTopicMetadata.newBuilder();
        partitionMetadataBuilder.setTopic(topic);
        partitionMetadataBuilder.setRequestId(requestId);
        CommandPartitionedTopicMetadata partitionMetadata = partitionMetadataBuilder.build();
        ByteBuf res = serializeWithSize(
                BaseCommand.newBuilder().setType(Type.PARTITIONED_METADATA).setPartitionMetadata(partitionMetadata));
        partitionMetadataBuilder.recycle();
        partitionMetadata.recycle();
        return res;
    }

    public static ByteBuf newPartitionMetadataResponse(int partitions, long requestId) {
        CommandPartitionedTopicMetadataResponse.Builder partitionMetadataResponseBuilder =
            CommandPartitionedTopicMetadataResponse.newBuilder();
        partitionMetadataResponseBuilder.setPartitions(partitions);
        partitionMetadataResponseBuilder.setResponse(CommandPartitionedTopicMetadataResponse.LookupType.Success);
        partitionMetadataResponseBuilder.setRequestId(requestId);

        CommandPartitionedTopicMetadataResponse partitionMetadataResponse = partitionMetadataResponseBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.PARTITIONED_METADATA_RESPONSE)
                .setPartitionMetadataResponse(partitionMetadataResponse));
        partitionMetadataResponseBuilder.recycle();
        partitionMetadataResponse.recycle();
        return res;
    }

    public static ByteBuf newLookup(String topic, boolean authoritative, long requestId) {
        return newLookup(topic, null, authoritative, requestId);
    }

    public static ByteBuf newLookup(String topic, String listenerName, boolean authoritative, long requestId) {
        CommandLookupTopic.Builder lookupTopicBuilder = CommandLookupTopic.newBuilder();
        lookupTopicBuilder.setTopic(topic);
        lookupTopicBuilder.setRequestId(requestId);
        lookupTopicBuilder.setAuthoritative(authoritative);
        if (StringUtils.isNotBlank(listenerName)) {
            lookupTopicBuilder.setAdvertisedListenerName(listenerName);
        }
        CommandLookupTopic lookupBroker = lookupTopicBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.LOOKUP).setLookupTopic(lookupBroker));
        lookupTopicBuilder.recycle();
        lookupBroker.recycle();
        return res;
    }

    public static ByteBuf newLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
            LookupType response, long requestId, boolean proxyThroughServiceUrl) {
        CommandLookupTopicResponse.Builder commandLookupTopicResponseBuilder = CommandLookupTopicResponse.newBuilder();
        commandLookupTopicResponseBuilder.setBrokerServiceUrl(brokerServiceUrl);
        if (brokerServiceUrlTls != null) {
            commandLookupTopicResponseBuilder.setBrokerServiceUrlTls(brokerServiceUrlTls);
        }
        commandLookupTopicResponseBuilder.setResponse(response);
        commandLookupTopicResponseBuilder.setRequestId(requestId);
        commandLookupTopicResponseBuilder.setAuthoritative(authoritative);
        commandLookupTopicResponseBuilder.setProxyThroughServiceUrl(proxyThroughServiceUrl);

        CommandLookupTopicResponse commandLookupTopicResponse = commandLookupTopicResponseBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.LOOKUP_RESPONSE)
                .setLookupTopicResponse(commandLookupTopicResponse));
        commandLookupTopicResponseBuilder.recycle();
        commandLookupTopicResponse.recycle();
        return res;
    }

    public static ByteBuf newLookupErrorResponse(ServerError error, String errorMsg, long requestId) {
        CommandLookupTopicResponse.Builder connectionBuilder = CommandLookupTopicResponse.newBuilder();
        connectionBuilder.setRequestId(requestId);
        connectionBuilder.setError(error);
        if (errorMsg != null) {
            connectionBuilder.setMessage(errorMsg);
        }
        connectionBuilder.setResponse(LookupType.Failed);

        CommandLookupTopicResponse connectionBroker = connectionBuilder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.LOOKUP_RESPONSE).setLookupTopicResponse(connectionBroker));
        connectionBuilder.recycle();
        connectionBroker.recycle();
        return res;
    }

    public static ByteBuf newMultiMessageAck(long consumerId,
             List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries) {
        CommandAck.Builder ackBuilder = CommandAck.newBuilder();
        ackBuilder.setConsumerId(consumerId);
        ackBuilder.setAckType(AckType.Individual);

        int entriesCount = entries.size();
        for (int i = 0; i < entriesCount; i++) {
            long ledgerId = entries.get(i).getLeft();
            long entryId = entries.get(i).getMiddle();
            ConcurrentBitSetRecyclable bitSet = entries.get(i).getRight();
            MessageIdData.Builder messageIdDataBuilder = MessageIdData.newBuilder();
            messageIdDataBuilder.setLedgerId(ledgerId);
            messageIdDataBuilder.setEntryId(entryId);
            if (bitSet != null) {
                messageIdDataBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(bitSet.toLongArray()));
            }
            MessageIdData messageIdData = messageIdDataBuilder.build();
            ackBuilder.addMessageId(messageIdData);
            if (bitSet != null) {
                bitSet.recycle();
            }
            messageIdDataBuilder.recycle();
        }

        CommandAck ack = ackBuilder.build();

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ACK).setAck(ack));

        for (int i = 0; i < entriesCount; i++) {
            ack.getMessageId(i).recycle();
        }
        ack.recycle();
        ackBuilder.recycle();
        return res;
    }

    public static ByteBuf newAck(long consumerId, long ledgerId, long entryId, BitSetRecyclable ackSet, AckType ackType,
                                 ValidationError validationError, Map<String, Long> properties) {
        return newAck(consumerId, ledgerId, entryId, ackSet, ackType, validationError, properties, -1, -1);
    }

    public static ByteBuf newAck(long consumerId, long ledgerId, long entryId, BitSetRecyclable ackSet, AckType ackType,
                                 ValidationError validationError, Map<String, Long> properties, long txnIdLeastBits,
                                 long txnIdMostBits) {
        CommandAck.Builder ackBuilder = CommandAck.newBuilder();
        ackBuilder.setConsumerId(consumerId);
        ackBuilder.setAckType(ackType);
        MessageIdData.Builder messageIdDataBuilder = MessageIdData.newBuilder();
        messageIdDataBuilder.setLedgerId(ledgerId);
        messageIdDataBuilder.setEntryId(entryId);
        if (ackSet != null) {
            messageIdDataBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(ackSet.toLongArray()));
        }
        MessageIdData messageIdData = messageIdDataBuilder.build();
        ackBuilder.addMessageId(messageIdData);
        if (validationError != null) {
            ackBuilder.setValidationError(validationError);
        }
        if (txnIdMostBits >= 0) {
            ackBuilder.setTxnidMostBits(txnIdMostBits);
        }
        if (txnIdLeastBits >= 0) {
            ackBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        for (Map.Entry<String, Long> e : properties.entrySet()) {
            ackBuilder.addProperties(
                    KeyLongValue.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build());
        }
        CommandAck ack = ackBuilder.build();

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ACK).setAck(ack));
        ack.recycle();
        ackBuilder.recycle();
        messageIdDataBuilder.recycle();
        messageIdData.recycle();
        return res;
    }

    public static ByteBuf newAckResponse(long consumerId, long txnIdLeastBits, long txnIdMostBits) {
        CommandAckResponse.Builder commandAckResponseBuilder = CommandAckResponse.newBuilder();
        commandAckResponseBuilder.setConsumerId(consumerId);
        commandAckResponseBuilder.setTxnidLeastBits(txnIdLeastBits);
        commandAckResponseBuilder.setTxnidMostBits(txnIdMostBits);
        CommandAckResponse commandAckResponse = commandAckResponseBuilder.build();

        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.ACK_RESPONSE).setAckResponse(commandAckResponse));
        commandAckResponseBuilder.recycle();
        commandAckResponse.recycle();

        return res;
    }

    public static ByteBuf newAckErrorResponse(ServerError error, String errorMsg, long consumerId) {
        CommandAckResponse.Builder ackErrorBuilder = CommandAckResponse.newBuilder();
        ackErrorBuilder.setConsumerId(consumerId);
        ackErrorBuilder.setError(error);
        if (errorMsg != null) {
            ackErrorBuilder.setMessage(errorMsg);
        }

        CommandAckResponse response = ackErrorBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ACK_RESPONSE).setAckResponse(response));

        ackErrorBuilder.recycle();
        response.recycle();

        return res;
    }

    public static ByteBuf newFlow(long consumerId, int messagePermits) {
        CommandFlow.Builder flowBuilder = CommandFlow.newBuilder();
        flowBuilder.setConsumerId(consumerId);
        flowBuilder.setMessagePermits(messagePermits);
        CommandFlow flow = flowBuilder.build();

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.FLOW).setFlow(flowBuilder));
        flow.recycle();
        flowBuilder.recycle();
        return res;
    }

    public static ByteBuf newRedeliverUnacknowledgedMessages(long consumerId) {
        CommandRedeliverUnacknowledgedMessages.Builder redeliverBuilder = CommandRedeliverUnacknowledgedMessages
                .newBuilder();
        redeliverBuilder.setConsumerId(consumerId);
        CommandRedeliverUnacknowledgedMessages redeliver = redeliverBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.REDELIVER_UNACKNOWLEDGED_MESSAGES)
                .setRedeliverUnacknowledgedMessages(redeliverBuilder));
        redeliver.recycle();
        redeliverBuilder.recycle();
        return res;
    }

    public static ByteBuf newRedeliverUnacknowledgedMessages(long consumerId, List<MessageIdData> messageIds) {
        CommandRedeliverUnacknowledgedMessages.Builder redeliverBuilder = CommandRedeliverUnacknowledgedMessages
                .newBuilder();
        redeliverBuilder.setConsumerId(consumerId);
        redeliverBuilder.addAllMessageIds(messageIds);
        CommandRedeliverUnacknowledgedMessages redeliver = redeliverBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.REDELIVER_UNACKNOWLEDGED_MESSAGES)
                .setRedeliverUnacknowledgedMessages(redeliverBuilder));
        redeliver.recycle();
        redeliverBuilder.recycle();
        return res;
    }

    public static ByteBuf newConsumerStatsResponse(ServerError serverError, String errMsg, long requestId) {
        CommandConsumerStatsResponse.Builder commandConsumerStatsResponseBuilder = CommandConsumerStatsResponse
                .newBuilder();
        commandConsumerStatsResponseBuilder.setRequestId(requestId);
        commandConsumerStatsResponseBuilder.setErrorMessage(errMsg);
        commandConsumerStatsResponseBuilder.setErrorCode(serverError);

        CommandConsumerStatsResponse commandConsumerStatsResponse = commandConsumerStatsResponseBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.CONSUMER_STATS_RESPONSE)
                .setConsumerStatsResponse(commandConsumerStatsResponseBuilder));
        commandConsumerStatsResponse.recycle();
        commandConsumerStatsResponseBuilder.recycle();
        return res;
    }

    public static ByteBuf newConsumerStatsResponse(CommandConsumerStatsResponse.Builder builder) {
        CommandConsumerStatsResponse commandConsumerStatsResponse = builder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.CONSUMER_STATS_RESPONSE)
                .setConsumerStatsResponse(builder));
        commandConsumerStatsResponse.recycle();
        builder.recycle();
        return res;
    }

    public static ByteBuf newGetTopicsOfNamespaceRequest(String namespace, long requestId, Mode mode) {
        CommandGetTopicsOfNamespace.Builder topicsBuilder = CommandGetTopicsOfNamespace.newBuilder();
        topicsBuilder.setNamespace(namespace).setRequestId(requestId).setMode(mode);

        CommandGetTopicsOfNamespace topicsCommand = topicsBuilder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.GET_TOPICS_OF_NAMESPACE).setGetTopicsOfNamespace(topicsCommand));
        topicsBuilder.recycle();
        topicsCommand.recycle();
        return res;
    }

    public static ByteBuf newGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        CommandGetTopicsOfNamespaceResponse.Builder topicsResponseBuilder =
            CommandGetTopicsOfNamespaceResponse.newBuilder();

        topicsResponseBuilder.setRequestId(requestId).addAllTopics(topics);

        CommandGetTopicsOfNamespaceResponse topicsOfNamespaceResponse = topicsResponseBuilder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_TOPICS_OF_NAMESPACE_RESPONSE)
            .setGetTopicsOfNamespaceResponse(topicsOfNamespaceResponse));

        topicsResponseBuilder.recycle();
        topicsOfNamespaceResponse.recycle();
        return res;
    }

    private final static ByteBuf cmdPing;

    static {
        ByteBuf serializedCmdPing = serializeWithSize(
                   BaseCommand.newBuilder()
                       .setType(Type.PING)
                       .setPing(CommandPing.getDefaultInstance()));
        cmdPing = Unpooled.copiedBuffer(serializedCmdPing);
        serializedCmdPing.release();
    }

    static ByteBuf newPing() {
        return cmdPing.retainedDuplicate();
    }

    private final static ByteBuf cmdPong;

    static {
        ByteBuf serializedCmdPong = serializeWithSize(
                   BaseCommand.newBuilder()
                       .setType(Type.PONG)
                       .setPong(CommandPong.getDefaultInstance()));
        cmdPong = Unpooled.copiedBuffer(serializedCmdPong);
        serializedCmdPong.release();
    }

    static ByteBuf newPong() {
        return cmdPong.retainedDuplicate();
    }

    public static ByteBuf newGetLastMessageId(long consumerId, long requestId) {
        CommandGetLastMessageId.Builder cmdBuilder = CommandGetLastMessageId.newBuilder();
        cmdBuilder.setConsumerId(consumerId).setRequestId(requestId);

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_LAST_MESSAGE_ID)
            .setGetLastMessageId(cmdBuilder.build()));
        cmdBuilder.recycle();
        return res;
    }

    public static ByteBuf newGetLastMessageIdResponse(long requestId, MessageIdData messageIdData) {
        PulsarApi.CommandGetLastMessageIdResponse.Builder response =
            PulsarApi.CommandGetLastMessageIdResponse.newBuilder()
            .setLastMessageId(messageIdData)
            .setRequestId(requestId);

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_LAST_MESSAGE_ID_RESPONSE)
            .setGetLastMessageIdResponse(response.build()));
        response.recycle();
        return res;
    }

    public static ByteBuf newGetSchema(long requestId, String topic, Optional<SchemaVersion> version) {
        CommandGetSchema.Builder schema = CommandGetSchema.newBuilder()
            .setRequestId(requestId);
        schema.setTopic(topic);
        version.ifPresent(schemaVersion -> schema.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes())));

        CommandGetSchema getSchema = schema.build();

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_SCHEMA)
            .setGetSchema(getSchema));
        schema.recycle();
        return res;
    }

    public static ByteBuf newGetSchemaResponse(long requestId, CommandGetSchemaResponse response) {
        CommandGetSchemaResponse.Builder schemaResponseBuilder = CommandGetSchemaResponse.newBuilder(response)
            .setRequestId(requestId);

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_SCHEMA_RESPONSE)
            .setGetSchemaResponse(schemaResponseBuilder.build()));
        schemaResponseBuilder.recycle();
        return res;
    }

    public static ByteBuf newGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        CommandGetSchemaResponse.Builder schemaResponse = CommandGetSchemaResponse.newBuilder()
            .setRequestId(requestId)
            .setSchemaVersion(ByteString.copyFrom(version.bytes()))
            .setSchema(getSchema(schema));

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_SCHEMA_RESPONSE)
            .setGetSchemaResponse(schemaResponse.build()));
        schemaResponse.recycle();
        return res;
    }

    public static ByteBuf newGetSchemaResponseError(long requestId, ServerError error, String errorMessage) {
        CommandGetSchemaResponse.Builder schemaResponse = CommandGetSchemaResponse.newBuilder()
            .setRequestId(requestId)
            .setErrorCode(error)
            .setErrorMessage(errorMessage);

        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
            .setType(Type.GET_SCHEMA_RESPONSE)
            .setGetSchemaResponse(schemaResponse.build()));
        schemaResponse.recycle();
        return res;
    }

    public static ByteBuf newGetOrCreateSchema(long requestId, String topic, SchemaInfo schemaInfo) {
        CommandGetOrCreateSchema getOrCreateSchema =
                CommandGetOrCreateSchema.newBuilder()
                                        .setRequestId(requestId)
                                        .setTopic(topic)
                                        .setSchema(getSchema(schemaInfo)).build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
                                                   .setType(Type.GET_OR_CREATE_SCHEMA)
                                                   .setGetOrCreateSchema(getOrCreateSchema));
        getOrCreateSchema.recycle();
        return res;
    }

    public static ByteBuf newGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        CommandGetOrCreateSchemaResponse.Builder schemaResponse =
                CommandGetOrCreateSchemaResponse.newBuilder()
                                                .setRequestId(requestId)
                                                .setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
                                                   .setType(Type.GET_OR_CREATE_SCHEMA_RESPONSE)
                                                   .setGetOrCreateSchemaResponse(schemaResponse.build()));
        schemaResponse.recycle();
        return res;
    }

    public static ByteBuf newGetOrCreateSchemaResponseError(long requestId, ServerError error, String errorMessage) {
        CommandGetOrCreateSchemaResponse.Builder schemaResponse =
                CommandGetOrCreateSchemaResponse.newBuilder()
                                                .setRequestId(requestId)
                                                .setErrorCode(error)
                                                .setErrorMessage(errorMessage);
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder()
                                                   .setType(Type.GET_OR_CREATE_SCHEMA_RESPONSE)
                                                   .setGetOrCreateSchemaResponse(schemaResponse.build()));
        schemaResponse.recycle();
        return res;
    }

    // ---- transaction related ----

    public static ByteBuf newTxn(long tcId, long requestId, long ttlSeconds) {
        CommandNewTxn commandNewTxn = CommandNewTxn.newBuilder().setTcId(tcId).setRequestId(requestId)
                .setTxnTtlSeconds(ttlSeconds)
                .build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.NEW_TXN).setNewTxn(commandNewTxn));
        commandNewTxn.recycle();
        return res;
    }

    public static ByteBuf newTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandNewTxnResponse commandNewTxnResponse = CommandNewTxnResponse.newBuilder().setRequestId(requestId)
                                                                           .setTxnidMostBits(txnIdMostBits)
                                                                           .setTxnidLeastBits(txnIdLeastBits).build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.NEW_TXN_RESPONSE).setNewTxnResponse(commandNewTxnResponse));
        commandNewTxnResponse.recycle();

        return res;
    }

    public static ByteBuf newTxnResponse(long requestId, long txnIdMostBits, ServerError error, String errorMsg) {
        CommandNewTxnResponse.Builder builder = CommandNewTxnResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidMostBits(txnIdMostBits);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandNewTxnResponse errorResponse = builder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.NEW_TXN_RESPONSE).setNewTxnResponse(errorResponse));
        builder.recycle();
        errorResponse.recycle();

        return res;
    }

    public static ByteBuf newAddPartitionToTxn(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                               List<String> partitions) {
        PulsarApi.CommandAddPartitionToTxn.Builder builder = CommandAddPartitionToTxn.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidLeastBits(txnIdLeastBits);
        builder.setTxnidMostBits(txnIdMostBits);
        if (partitions != null) {
            builder.addAllPartitions(partitions);
        }
        CommandAddPartitionToTxn commandAddPartitionToTxn = builder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.ADD_PARTITION_TO_TXN).setAddPartitionToTxn(commandAddPartitionToTxn));
        commandAddPartitionToTxn.recycle();
        return res;
    }

    public static ByteBuf newAddPartitionToTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandAddPartitionToTxnResponse commandAddPartitionToTxnResponse = CommandAddPartitionToTxnResponse
                                                                                .newBuilder().setRequestId(requestId)
                                                                                .setTxnidLeastBits(txnIdLeastBits)
                                                                                .setTxnidMostBits(txnIdMostBits)
                                                                                .build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ADD_PARTITION_TO_TXN_RESPONSE)
                                                   .setAddPartitionToTxnResponse(commandAddPartitionToTxnResponse));
        commandAddPartitionToTxnResponse.recycle();
        return res;
    }

    public static ByteBuf newAddPartitionToTxnResponse(long requestId, long txnIdMostBits, ServerError error,
           String errorMsg) {
        CommandAddPartitionToTxnResponse.Builder builder = CommandAddPartitionToTxnResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidMostBits(txnIdMostBits);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandAddPartitionToTxnResponse commandAddPartitionToTxnResponse = builder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ADD_PARTITION_TO_TXN_RESPONSE)
                                                   .setAddPartitionToTxnResponse(commandAddPartitionToTxnResponse));
        builder.recycle();
        commandAddPartitionToTxnResponse.recycle();
        return res;
    }

    public static ByteBuf newAddSubscriptionToTxn(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                                  List<Subscription> subscription) {
        CommandAddSubscriptionToTxn commandAddSubscriptionToTxn =
            CommandAddSubscriptionToTxn.newBuilder()
                                       .setRequestId(requestId)
                                       .setTxnidLeastBits(txnIdLeastBits)
                                       .setTxnidMostBits(txnIdMostBits)
                                       .addAllSubscription(subscription).build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ADD_SUBSCRIPTION_TO_TXN)
                                                   .setAddSubscriptionToTxn(commandAddSubscriptionToTxn));
        commandAddSubscriptionToTxn.recycle();
        return res;
    }

    public static ByteBuf newAddSubscriptionToTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandAddSubscriptionToTxnResponse command = CommandAddSubscriptionToTxnResponse
                .newBuilder().setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ADD_SUBSCRIPTION_TO_TXN_RESPONSE)
                .setAddSubscriptionToTxnResponse(command));
        command.recycle();
        return res;
    }

    public static ByteBuf newAddSubscriptionToTxnResponse(long requestId, long txnIdMostBits, ServerError error,
          String errorMsg) {
        CommandAddSubscriptionToTxnResponse.Builder builder = CommandAddSubscriptionToTxnResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidMostBits(txnIdMostBits);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandAddSubscriptionToTxnResponse errorResponse = builder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.ADD_SUBSCRIPTION_TO_TXN_RESPONSE)
                                                   .setAddSubscriptionToTxnResponse(errorResponse));
        builder.recycle();
        errorResponse.recycle();
        return res;
    }

    public static ByteBuf newEndTxn(long requestId, long txnIdLeastBits, long txnIdMostBits, TxnAction txnAction) {
        CommandEndTxn commandEndTxn = CommandEndTxn.newBuilder().setRequestId(requestId)
                                                   .setTxnidLeastBits(txnIdLeastBits).setTxnidMostBits(txnIdMostBits)
                                                   .setTxnAction(txnAction).build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN).setEndTxn(commandEndTxn));
        commandEndTxn.recycle();
        return res;
    }

    public static ByteBuf newEndTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandEndTxnResponse commandEndTxnResponse = CommandEndTxnResponse.newBuilder().setRequestId(requestId)
                                                                           .setTxnidLeastBits(txnIdLeastBits)
                                                                           .setTxnidMostBits(txnIdMostBits).build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.END_TXN_RESPONSE).setEndTxnResponse(commandEndTxnResponse));
        commandEndTxnResponse.recycle();
        return res;
    }

    public static ByteBuf newEndTxnResponse(long requestId, long txnIdMostBits, ServerError error, String errorMsg) {
        CommandEndTxnResponse.Builder builder = CommandEndTxnResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidMostBits(txnIdMostBits);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandEndTxnResponse commandEndTxnResponse = builder.build();
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.END_TXN_RESPONSE).setEndTxnResponse(commandEndTxnResponse));
        builder.recycle();
        commandEndTxnResponse.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnPartition(long requestId, long txnIdLeastBits, long txnIdMostBits, String topic,
                                               TxnAction txnAction) {
        CommandEndTxnOnPartition.Builder txnEndOnPartition = CommandEndTxnOnPartition.newBuilder()
                                                                                     .setRequestId(requestId)
                                                                                     .setTxnidLeastBits(txnIdLeastBits)
                                                                                     .setTxnidMostBits(txnIdMostBits)
                                                                                     .setTopic(topic)
                                                                                     .setTxnAction(txnAction);
        ByteBuf res = serializeWithSize(
            BaseCommand.newBuilder().setType(Type.END_TXN_ON_PARTITION).setEndTxnOnPartition(txnEndOnPartition));
        txnEndOnPartition.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnPartitionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandEndTxnOnPartitionResponse commandEndTxnOnPartitionResponse = CommandEndTxnOnPartitionResponse
                                                                                .newBuilder().setRequestId(requestId)
                                                                                .setTxnidLeastBits(txnIdLeastBits)
                                                                                .setTxnidMostBits(txnIdMostBits)
                                                                                .build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN_ON_PARTITION_RESPONSE)
                                                   .setEndTxnOnPartitionResponse(commandEndTxnOnPartitionResponse));
        commandEndTxnOnPartitionResponse.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnPartitionResponse(long requestId, ServerError error, String errorMsg) {
        CommandEndTxnOnPartitionResponse.Builder builder = CommandEndTxnOnPartitionResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandEndTxnOnPartitionResponse commandEndTxnOnPartitionResponse = builder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN_ON_PARTITION_RESPONSE)
                                                   .setEndTxnOnPartitionResponse(commandEndTxnOnPartitionResponse));
        builder.recycle();
        commandEndTxnOnPartitionResponse.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnSubscription(long requestId, long txnIdLeastBits, long txnIdMostBits, String topic,
                                                  String subscription, TxnAction txnAction) {
        Subscription.Builder builder = Subscription.newBuilder();
        builder.setTopic(topic);
        builder.setSubscription(subscription);
        Subscription sub = builder.build();
        builder.recycle();
        return newEndTxnOnSubscription(requestId, txnIdLeastBits, txnIdMostBits, sub, txnAction);
    }

    public static ByteBuf newEndTxnOnSubscription(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                                  Subscription subscription, TxnAction txnAction) {
        CommandEndTxnOnSubscription commandEndTxnOnSubscription =
            CommandEndTxnOnSubscription.newBuilder()
                                       .setRequestId(requestId)
                                       .setTxnidLeastBits(txnIdLeastBits)
                                       .setTxnidMostBits(txnIdMostBits)
                                       .setSubscription(subscription)
                                       .setTxnAction(txnAction)
                                       .build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN_ON_SUBSCRIPTION)
                                                   .setEndTxnOnSubscription(commandEndTxnOnSubscription));
        subscription.recycle();
        commandEndTxnOnSubscription.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnSubscriptionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        CommandEndTxnOnSubscriptionResponse response =
            CommandEndTxnOnSubscriptionResponse.newBuilder()
                                               .setRequestId(requestId)
                                               .setTxnidLeastBits(txnIdLeastBits)
                                               .setTxnidMostBits(txnIdMostBits).build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN_ON_SUBSCRIPTION_RESPONSE)
                                                   .setEndTxnOnSubscriptionResponse(response));
        response.recycle();
        return res;
    }

    public static ByteBuf newEndTxnOnSubscriptionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                                          ServerError error, String errorMsg) {
        CommandEndTxnOnSubscriptionResponse.Builder builder = CommandEndTxnOnSubscriptionResponse.newBuilder();
        builder.setRequestId(requestId);
        builder.setTxnidMostBits(txnIdMostBits);
        builder.setTxnidLeastBits(txnIdLeastBits);
        builder.setError(error);
        if (errorMsg != null) {
            builder.setMessage(errorMsg);
        }
        CommandEndTxnOnSubscriptionResponse response = builder.build();
        ByteBuf res = serializeWithSize(BaseCommand.newBuilder().setType(Type.END_TXN_ON_SUBSCRIPTION_RESPONSE)
                                                   .setEndTxnOnSubscriptionResponse(response));
        builder.recycle();
        response.recycle();
        return res;
    }
    @VisibleForTesting
    public static ByteBuf serializeWithSize(BaseCommand.Builder cmdBuilder) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD]
        BaseCommand cmd = cmdBuilder.build();

        int cmdSize = cmd.getSerializedSize();
        int totalSize = cmdSize + 4;
        int frameSize = totalSize + 4;

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(frameSize, frameSize);

        // Prepend 2 lengths to the buffer
        buf.writeInt(totalSize);
        buf.writeInt(cmdSize);

        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);

        try {
            cmd.writeTo(outStream);
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        } finally {
            cmd.recycle();
            cmdBuilder.recycle();
            outStream.recycle();
        }

        return buf;
    }

    private static ByteBufPair serializeCommandSendWithSize(BaseCommand.Builder cmdBuilder, ChecksumType checksumType,
            MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]

        BaseCommand cmd = cmdBuilder.build();
        int cmdSize = cmd.getSerializedSize();
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int payloadSize = payload.readableBytes();
        int magicAndChecksumLength = ChecksumType.Crc32c.equals(checksumType) ? (2 + 4 /* magic + checksumLength*/) : 0;
        boolean includeChecksum = magicAndChecksumLength > 0;
        // cmdLength + cmdSize + magicLength +
        // checksumSize + msgMetadataLength +
        // msgMetadataSize
        int headerContentSize = 4 + cmdSize + magicAndChecksumLength + 4 + msgMetadataSize;
        int totalSize = headerContentSize + payloadSize;
        int headersSize = 4 + headerContentSize; // totalSize + headerLength
        int checksumReaderIndex = -1;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.buffer(headersSize, headersSize);
        headers.writeInt(totalSize); // External frame

        try {
            // Write cmd
            headers.writeInt(cmdSize);

            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(headers);
            cmd.writeTo(outStream);
            cmd.recycle();
            cmdBuilder.recycle();

            //Create checksum placeholder
            if (includeChecksum) {
                headers.writeShort(magicCrc32c);
                checksumReaderIndex = headers.writerIndex();
                headers.writerIndex(headers.writerIndex() + checksumSize); //skip 4 bytes of checksum
            }

            // Write metadata
            headers.writeInt(msgMetadataSize);
            msgMetadata.writeTo(outStream);
            outStream.recycle();
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        ByteBufPair command = ByteBufPair.get(headers, payload);

        // write checksum at created checksum-placeholder
        if (includeChecksum) {
            headers.markReaderIndex();
            headers.readerIndex(checksumReaderIndex + checksumSize);
            int metadataChecksum = computeChecksum(headers);
            int computedChecksum = resumeChecksum(metadataChecksum, payload);
            // set computed checksum
            headers.setInt(checksumReaderIndex, computedChecksum);
            headers.resetReaderIndex();
        }
        return command;
    }

    public static ByteBuf serializeMetadataAndPayload(ChecksumType checksumType,
                                                      MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int payloadSize = payload.readableBytes();
        int magicAndChecksumLength = ChecksumType.Crc32c.equals(checksumType) ? (2 + 4 /* magic + checksumLength*/) : 0;
        boolean includeChecksum = magicAndChecksumLength > 0;
        int headerContentSize = magicAndChecksumLength + 4 + msgMetadataSize; // magicLength +
                                                                              // checksumSize + msgMetadataLength +
                                                                              // msgMetadataSize
        int checksumReaderIndex = -1;
        int totalSize = headerContentSize + payloadSize;

        ByteBuf metadataAndPayload = PulsarByteBufAllocator.DEFAULT.buffer(totalSize, totalSize);
        try {
            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(metadataAndPayload);

            //Create checksum placeholder
            if (includeChecksum) {
                metadataAndPayload.writeShort(magicCrc32c);
                checksumReaderIndex = metadataAndPayload.writerIndex();
                metadataAndPayload.writerIndex(metadataAndPayload.writerIndex()
                                               + checksumSize); //skip 4 bytes of checksum
            }

            // Write metadata
            metadataAndPayload.writeInt(msgMetadataSize);
            msgMetadata.writeTo(outStream);
            outStream.recycle();
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        // write checksum at created checksum-placeholder
        if (includeChecksum) {
            metadataAndPayload.markReaderIndex();
            metadataAndPayload.readerIndex(checksumReaderIndex + checksumSize);
            int metadataChecksum = computeChecksum(metadataAndPayload);
            int computedChecksum = resumeChecksum(metadataChecksum, payload);
            // set computed checksum
            metadataAndPayload.setInt(checksumReaderIndex, computedChecksum);
            metadataAndPayload.resetReaderIndex();
        }
        metadataAndPayload.writeBytes(payload);

        return metadataAndPayload;
    }

    public static long initBatchMessageMetadata(MessageMetadata.Builder messageMetadata,
            MessageMetadata.Builder builder) {
        messageMetadata.setPublishTime(builder.getPublishTime());
        messageMetadata.setProducerName(builder.getProducerName());
        messageMetadata.setSequenceId(builder.getSequenceId());
        // Attach the key to the message metadata.
        if (builder.hasPartitionKey()) {
            messageMetadata.setPartitionKey(builder.getPartitionKey());
            messageMetadata.setPartitionKeyB64Encoded(builder.getPartitionKeyB64Encoded());
        }
        if (builder.hasOrderingKey()) {
            messageMetadata.setOrderingKey(builder.getOrderingKey());
        }
        if (builder.hasReplicatedFrom()) {
            messageMetadata.setReplicatedFrom(builder.getReplicatedFrom());
        }
        if (builder.getReplicateToCount() > 0) {
            messageMetadata.addAllReplicateTo(builder.getReplicateToList());
        }
        if (builder.hasSchemaVersion()) {
            messageMetadata.setSchemaVersion(builder.getSchemaVersion());
        }
        return builder.getSequenceId();
    }

    public static ByteBuf serializeSingleMessageInBatchWithPayload(
            SingleMessageMetadata.Builder singleMessageMetadataBuilder,
            ByteBuf payload, ByteBuf batchBuffer) {
        int payLoadSize = payload.readableBytes();
        SingleMessageMetadata singleMessageMetadata = singleMessageMetadataBuilder.setPayloadSize(payLoadSize)
                .build();
        // serialize meta-data size, meta-data and payload for single message in batch
        int singleMsgMetadataSize = singleMessageMetadata.getSerializedSize();
        try {
            batchBuffer.writeInt(singleMsgMetadataSize);
            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(batchBuffer);
            singleMessageMetadata.writeTo(outStream);
            singleMessageMetadata.recycle();
            outStream.recycle();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return batchBuffer.writeBytes(payload);
    }

    public static ByteBuf serializeSingleMessageInBatchWithPayload(MessageMetadata.Builder msgBuilder,
            ByteBuf payload, ByteBuf batchBuffer) {

        // build single message meta-data
        SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata
                .newBuilder();
        if (msgBuilder.hasPartitionKey()) {
            singleMessageMetadataBuilder = singleMessageMetadataBuilder.setPartitionKey(msgBuilder.getPartitionKey())
                .setPartitionKeyB64Encoded(msgBuilder.getPartitionKeyB64Encoded());
        }
        if (msgBuilder.hasOrderingKey()) {
            singleMessageMetadataBuilder = singleMessageMetadataBuilder.setOrderingKey(msgBuilder.getOrderingKey());
        }
        if (!msgBuilder.getPropertiesList().isEmpty()) {
            singleMessageMetadataBuilder = singleMessageMetadataBuilder
                    .addAllProperties(msgBuilder.getPropertiesList());
        }

        if (msgBuilder.hasEventTime()) {
            singleMessageMetadataBuilder.setEventTime(msgBuilder.getEventTime());
        }

        if (msgBuilder.hasSequenceId()) {
            singleMessageMetadataBuilder.setSequenceId(msgBuilder.getSequenceId());
        }

        if (msgBuilder.hasNullValue()) {
            singleMessageMetadataBuilder.setNullValue(msgBuilder.hasNullValue());
        }

        if (msgBuilder.hasNullPartitionKey()) {
            singleMessageMetadataBuilder.setNullPartitionKey(msgBuilder.hasNullPartitionKey());
        }

        try {
            return serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder, payload, batchBuffer);
        } finally {
            singleMessageMetadataBuilder.recycle();
        }
    }

    public static ByteBuf deSerializeSingleMessageInBatch(ByteBuf uncompressedPayload,
            SingleMessageMetadata.Builder singleMessageMetadataBuilder, int index, int batchSize)
            throws IOException {
        int singleMetaSize = (int) uncompressedPayload.readUnsignedInt();
        int writerIndex = uncompressedPayload.writerIndex();
        int beginIndex = uncompressedPayload.readerIndex() + singleMetaSize;
        uncompressedPayload.writerIndex(beginIndex);
        ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(uncompressedPayload);
        singleMessageMetadataBuilder.mergeFrom(stream, null);
        stream.recycle();

        int singleMessagePayloadSize = singleMessageMetadataBuilder.getPayloadSize();

        int readerIndex = uncompressedPayload.readerIndex();
        ByteBuf singleMessagePayload = uncompressedPayload.retainedSlice(readerIndex, singleMessagePayloadSize);
        uncompressedPayload.writerIndex(writerIndex);

        // reader now points to beginning of payload read; so move it past message payload just read
        if (index < batchSize) {
            uncompressedPayload.readerIndex(readerIndex + singleMessagePayloadSize);
        }

        return singleMessagePayload;
    }

    private static ByteBufPair serializeCommandMessageWithSize(BaseCommand cmd, ByteBuf metadataAndPayload) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        //
        // metadataAndPayload contains from magic-number to the payload included


        int cmdSize = cmd.getSerializedSize();
        int totalSize = 4 + cmdSize + metadataAndPayload.readableBytes();
        int headersSize = 4 + 4 + cmdSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.buffer(headersSize);
        headers.writeInt(totalSize); // External frame

        try {
            // Write cmd
            headers.writeInt(cmdSize);

            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(headers);
            cmd.writeTo(outStream);
            outStream.recycle();
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        return ByteBufPair.get(headers, metadataAndPayload);
    }

    public static int getNumberOfMessagesInBatch(ByteBuf metadataAndPayload, String subscription,
            long consumerId) {
        MessageMetadata msgMetadata = peekMessageMetadata(metadataAndPayload, subscription, consumerId);
        if (msgMetadata == null) {
            return -1;
        } else {
            int numMessagesInBatch = msgMetadata.getNumMessagesInBatch();
            msgMetadata.recycle();
            return numMessagesInBatch;
        }
    }

    public static MessageMetadata peekMessageMetadata(ByteBuf metadataAndPayload, String subscription,
            long consumerId) {
        try {
            // save the reader index and restore after parsing
            int readerIdx = metadataAndPayload.readerIndex();
            MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);
            metadataAndPayload.readerIndex(readerIdx);

            return metadata;
        } catch (Throwable t) {
            log.error("[{}] [{}] Failed to parse message metadata", subscription, consumerId, t);
            return null;
        }
    }

    public static int getCurrentProtocolVersion() {
        // Return the last ProtocolVersion enum value
        return ProtocolVersion.values()[ProtocolVersion.values().length - 1].getNumber();
    }

    /**
     * Definition of possible checksum types.
     */
    public enum ChecksumType {
        Crc32c,
        None;
    }

    public static boolean peerSupportsGetLastMessageId(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getNumber();
    }

    public static boolean peerSupportsActiveConsumerListener(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getNumber();
    }

    public static boolean peerSupportsMultiMessageAcknowledgment(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getNumber();
    }

    public static boolean peerSupportJsonSchemaAvroFormat(int peerVersion) {
        return peerVersion >= ProtocolVersion.v13.getNumber();
    }

    public static boolean peerSupportsGetOrCreateSchema(int peerVersion) {
        return peerVersion >= ProtocolVersion.v15.getNumber();
    }
}
