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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.AuthMethod;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAck.ValidationError;
import org.apache.pulsar.common.api.proto.CommandAckResponse;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxn;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxn;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageIdResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.CommandSeek;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectResponse;
import org.apache.pulsar.common.api.proto.FeatureFlags;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.Schema;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;

@UtilityClass
@Slf4j
@SuppressWarnings("checkstyle:JavadocType")
public class Commands {

    // default message size for transfer
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 5 * 1024 * 1024;
    public static final int MESSAGE_SIZE_FRAME_PADDING = 10 * 1024;
    public static final int INVALID_MAX_MESSAGE_SIZE = -1;

    // this present broker version don't have consumerEpoch feature,
    // so client don't need to think about consumerEpoch feature
    public static final long DEFAULT_CONSUMER_EPOCH = -1L;

    @SuppressWarnings("checkstyle:ConstantName")
    public static final short magicCrc32c = 0x0e01;
    @SuppressWarnings("checkstyle:ConstantName")
    public static final short magicBrokerEntryMetadata = 0x0e02;
    private static final int checksumSize = 4;

    private static final FastThreadLocal<BaseCommand> LOCAL_BASE_COMMAND = new FastThreadLocal<BaseCommand>() {
        @Override
        protected BaseCommand initialValue() throws Exception {
            return new BaseCommand();
        }
    };

    // Return the last ProtocolVersion enum value
    private static final int CURRENT_PROTOCOL_VERSION =
            ProtocolVersion.values()[ProtocolVersion.values().length - 1].getValue();

    private static BaseCommand localCmd(BaseCommand.Type type) {
        return LOCAL_BASE_COMMAND.get()
                .clear()
                .setType(type);
    }

    private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA = //
            new FastThreadLocal<SingleMessageMetadata>() {
                @Override
                protected SingleMessageMetadata initialValue() throws Exception {
                    return new SingleMessageMetadata();
                }
            };

    private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA = //
            new FastThreadLocal<MessageMetadata>() {
                @Override
                protected MessageMetadata initialValue() throws Exception {
                    return new MessageMetadata();
                }
            };

    private static final FastThreadLocal<BrokerEntryMetadata> BROKER_ENTRY_METADATA = //
            new FastThreadLocal<BrokerEntryMetadata>() {
                @Override
                protected BrokerEntryMetadata initialValue() throws Exception {
                    return new BrokerEntryMetadata();
                }
            };

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

    private static void setFeatureFlags(FeatureFlags flags) {
        flags.setSupportsAuthRefresh(true);
        flags.setSupportsBrokerEntryMetadata(true);
        flags.setSupportsPartialProducer(true);
    }

    public static ByteBuf newConnect(String authMethodName, String authData, int protocolVersion, String libVersion,
            String targetBroker, String originalPrincipal, String originalAuthData,
            String originalAuthMethod) {
        BaseCommand cmd = localCmd(Type.CONNECT);
        CommandConnect connect = cmd.setConnect()
                .setClientVersion(libVersion != null ? libVersion : "Pulsar Client")
                .setAuthMethodName(authMethodName);

        if ("ycav1".equals(authMethodName)) {
            // Handle the case of a client that gets updated before the broker and starts sending the string auth method
            // name. An example would be in broker-to-broker replication. We need to make sure the clients are still
            // passing both the enum and the string until all brokers are upgraded.
            connect.setAuthMethod(AuthMethod.AuthMethodYcaV1);
        }

        if (targetBroker != null) {
            // When connecting through a proxy, we need to specify which broker do we want to be proxied through
            connect.setProxyToBrokerUrl(targetBroker);
        }

        if (authData != null) {
            connect.setAuthData(authData.getBytes(UTF_8));
        }

        if (originalPrincipal != null) {
            connect.setOriginalPrincipal(originalPrincipal);
        }

        if (originalAuthData != null) {
            connect.setOriginalAuthData(originalAuthData);
        }

        if (originalAuthMethod != null) {
            connect.setOriginalAuthMethod(originalAuthMethod);
        }
        connect.setProtocolVersion(protocolVersion);

        setFeatureFlags(connect.setFeatureFlags());
        return serializeWithSize(cmd);
    }

    public static ByteBuf newConnect(String authMethodName, AuthData authData, int protocolVersion, String libVersion,
                                     String targetBroker, String originalPrincipal, AuthData originalAuthData,
                                     String originalAuthMethod) {
        BaseCommand cmd = localCmd(Type.CONNECT);
        CommandConnect connect = cmd.setConnect()
                .setClientVersion(libVersion != null ? libVersion : "Pulsar Client")
                .setAuthMethodName(authMethodName);

        if (targetBroker != null) {
            // When connecting through a proxy, we need to specify which broker do we want to be proxied through
            connect.setProxyToBrokerUrl(targetBroker);
        }

        if (authData != null) {
            connect.setAuthData(authData.getBytes());
        }

        if (originalPrincipal != null) {
            connect.setOriginalPrincipal(originalPrincipal);
        }

        if (originalAuthData != null) {
            connect.setOriginalAuthData(new String(originalAuthData.getBytes(), UTF_8));
        }

        if (originalAuthMethod != null) {
            connect.setOriginalAuthMethod(originalAuthMethod);
        }
        connect.setProtocolVersion(protocolVersion);
        setFeatureFlags(connect.setFeatureFlags());

        return serializeWithSize(cmd);
    }

    public static ByteBuf newConnected(int clientProtocoVersion) {
        return newConnected(clientProtocoVersion, INVALID_MAX_MESSAGE_SIZE);
    }

    public static BaseCommand newConnectedCommand(int clientProtocolVersion, int maxMessageSize) {
        BaseCommand cmd = localCmd(Type.CONNECTED);
        CommandConnected connected = cmd.setConnected()
                .setServerVersion("Pulsar Server" + PulsarVersion.getVersion());

        if (INVALID_MAX_MESSAGE_SIZE != maxMessageSize) {
            connected.setMaxMessageSize(maxMessageSize);
        }

        // If the broker supports a newer version of the protocol, it will anyway advertise the max version that the
        // client supports, to avoid confusing the client.
        int currentProtocolVersion = getCurrentProtocolVersion();
        int versionToAdvertise = Math.min(currentProtocolVersion, clientProtocolVersion);

        connected.setProtocolVersion(versionToAdvertise);
        return cmd;
    }

    public static ByteBuf newConnected(int clientProtocolVersion, int maxMessageSize) {
        return serializeWithSize(newConnectedCommand(clientProtocolVersion, maxMessageSize));
    }

    public static ByteBuf newAuthChallenge(String authMethod, AuthData brokerData, int clientProtocolVersion) {
        BaseCommand cmd = localCmd(Type.AUTH_CHALLENGE);
        CommandAuthChallenge challenge = cmd.setAuthChallenge();

        // If the broker supports a newer version of the protocol, it will anyway advertise the max version that the
        // client supports, to avoid confusing the client.
        int currentProtocolVersion = getCurrentProtocolVersion();
        int versionToAdvertise = Math.min(currentProtocolVersion, clientProtocolVersion);

        challenge.setProtocolVersion(versionToAdvertise)
                .setChallenge()
                .setAuthData(brokerData != null ? brokerData.getBytes() : new byte[0])
                .setAuthMethodName(authMethod);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAuthResponse(String authMethod,
                                           AuthData clientData,
                                           int clientProtocolVersion,
                                           String clientVersion) {
        BaseCommand cmd = localCmd(Type.AUTH_RESPONSE);
        cmd.setAuthResponse()
                .setClientVersion(clientVersion != null ? clientVersion : "Pulsar Client")
                .setProtocolVersion(clientProtocolVersion)
                .setResponse()
                .setAuthData(clientData.getBytes())
                .setAuthMethodName(authMethod);
        return serializeWithSize(cmd);
    }

    public static BaseCommand newSuccessCommand(long requestId) {
        BaseCommand cmd = localCmd(Type.SUCCESS);
        cmd.setSuccess()
                .setRequestId(requestId);
        return cmd;
    }

    public static ByteBuf newSuccess(long requestId) {
        return serializeWithSize(newSuccessCommand(requestId));
    }

    public static BaseCommand newProducerSuccessCommand(long requestId, String producerName,
            SchemaVersion schemaVersion) {
        return newProducerSuccessCommand(requestId, producerName, -1, schemaVersion, Optional.empty(), true);
    }

    public static ByteBuf newProducerSuccess(long requestId, String producerName, SchemaVersion schemaVersion) {
        return newProducerSuccess(requestId, producerName, -1, schemaVersion, Optional.empty(), true);
    }

    public static BaseCommand newProducerSuccessCommand(long requestId, String producerName, long lastSequenceId,
            SchemaVersion schemaVersion, Optional<Long> topicEpoch, boolean isProducerReady) {
        BaseCommand cmd = localCmd(Type.PRODUCER_SUCCESS);
        CommandProducerSuccess ps = cmd.setProducerSuccess()
                .setRequestId(requestId)
                .setProducerName(producerName)
                .setLastSequenceId(lastSequenceId)
                .setSchemaVersion(schemaVersion.bytes())
                .setProducerReady(isProducerReady);
        topicEpoch.ifPresent(ps::setTopicEpoch);
        return cmd;
    }

    public static ByteBuf newProducerSuccess(long requestId, String producerName, long lastSequenceId,
            SchemaVersion schemaVersion, Optional<Long> topicEpoch, boolean isProducerReady) {
        return serializeWithSize(newProducerSuccessCommand(requestId, producerName, lastSequenceId, schemaVersion,
                topicEpoch, isProducerReady));
    }

    public static BaseCommand newErrorCommand(long requestId, ServerError serverError, String message) {
        BaseCommand cmd = localCmd(Type.ERROR);
        cmd.setError()
                .setRequestId(requestId)
                .setError(serverError)
                .setMessage(message != null ? message : "");
        return cmd;
    }

    public static ByteBuf newError(long requestId, ServerError serverError, String message) {
        return serializeWithSize(newErrorCommand(requestId, serverError, message));
    }

    public static BaseCommand newSendReceiptCommand(long producerId, long sequenceId, long highestId, long ledgerId,
            long entryId) {
        BaseCommand cmd = localCmd(Type.SEND_RECEIPT);
        cmd.setSendReceipt()
                .setProducerId(producerId)
                .setSequenceId(sequenceId)
                .setHighestSequenceId(highestId)
                .setMessageId()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        return cmd;
    }

    public static ByteBuf newSendReceipt(long producerId, long sequenceId, long highestId, long ledgerId,
            long entryId) {
        return serializeWithSize(newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId, entryId));
    }

    public static BaseCommand newSendErrorCommand(long producerId, long sequenceId, ServerError error,
            String errorMsg) {
        BaseCommand cmd = localCmd(Type.SEND_ERROR);
        cmd.setSendError()
                .setProducerId(producerId)
                .setSequenceId(sequenceId)
                .setError(error)
                .setMessage(errorMsg != null ? errorMsg : "");
        return cmd;
    }

    public static ByteBuf newSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
        return serializeWithSize(newSendErrorCommand(producerId, sequenceId, error, errorMsg));
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
        MessageMetadata md = LOCAL_MESSAGE_METADATA.get();
        parseMessageMetadata(buffer, md);
        return md;
    }

    public static void parseMessageMetadata(ByteBuf buffer, MessageMetadata msgMetadata) {
        // initially reader-index may point to start of broker entry metadata :
        // increment reader-index to start_of_headAndPayload to parse metadata
        skipBrokerEntryMetadataIfExist(buffer);
        // initially reader-index may point to start_of_checksum : increment reader-index to start_of_metadata
        // to parse metadata
        skipChecksumIfPresent(buffer);
        int metadataSize = (int) buffer.readUnsignedInt();

        msgMetadata.parseFrom(buffer, metadataSize);
    }

    public static void skipMessageMetadata(ByteBuf buffer) {
        // initially reader-index may point to start_of_checksum : increment reader-index to start_of_metadata to parse
        // metadata
        skipBrokerEntryMetadataIfExist(buffer);
        skipChecksumIfPresent(buffer);
        int metadataSize = (int) buffer.readUnsignedInt();
        buffer.skipBytes(metadataSize);
    }

    public static long getEntryTimestamp(ByteBuf headersAndPayloadWithBrokerEntryMetadata) throws IOException {
        // get broker timestamp first if BrokerEntryMetadata is enabled with AppendBrokerTimestampMetadataInterceptor
        BrokerEntryMetadata brokerEntryMetadata =
                Commands.parseBrokerEntryMetadataIfExist(headersAndPayloadWithBrokerEntryMetadata);
        if (brokerEntryMetadata != null && brokerEntryMetadata.hasBrokerTimestamp()) {
            return brokerEntryMetadata.getBrokerTimestamp();
        }
        // otherwise get the publish_time
        return parseMessageMetadata(headersAndPayloadWithBrokerEntryMetadata).getPublishTime();
    }

    public static BaseCommand newMessageCommand(long consumerId, long ledgerId, long entryId, int partition,
            int redeliveryCount, long[] ackSet, long consumerEpoch) {
        BaseCommand cmd = localCmd(Type.MESSAGE);
        CommandMessage msg = cmd.setMessage()
                .setConsumerId(consumerId);
        msg.setMessageId()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setPartition(partition);

        // consumerEpoch > -1 is useful
        if (consumerEpoch > DEFAULT_CONSUMER_EPOCH) {
            msg.setConsumerEpoch(consumerEpoch);
        }
        if (redeliveryCount > 0) {
            msg.setRedeliveryCount(redeliveryCount);
        }
        if (ackSet != null) {
            for (int i = 0; i < ackSet.length; i++) {
                msg.addAckSet(ackSet[i]);
            }
        }
        return cmd;
    }

    public static ByteBufPair newMessage(long consumerId, long ledgerId, long entryId, int partition,
            int redeliveryCount, ByteBuf metadataAndPayload, long[] ackSet) {
        return serializeCommandMessageWithSize(
                newMessageCommand(consumerId, ledgerId, entryId, partition, redeliveryCount, ackSet,
                        DEFAULT_CONSUMER_EPOCH), metadataAndPayload);
    }

    public static ByteBufPair newSend(long producerId, long sequenceId, int numMessaegs, ChecksumType checksumType,
                                      MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(producerId, sequenceId, -1 /* highestSequenceId */, numMessaegs,
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

    public static ByteBufPair newSend(long producerId, long sequenceId, long highestSequenceId, int numMessages,
                                      long txnIdLeastBits, long txnIdMostBits, ChecksumType checksumType,
            MessageMetadata messageData, ByteBuf payload) {
        BaseCommand cmd = localCmd(Type.SEND);
        CommandSend send = cmd.setSend()
                .setProducerId(producerId)
                .setSequenceId(sequenceId);
        if (highestSequenceId >= 0) {
            send.setHighestSequenceId(highestSequenceId);
        }
        if (numMessages > 1) {
            send.setNumMessages(numMessages);
        }
        if (txnIdLeastBits >= 0) {
            send.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits >= 0) {
            send.setTxnidMostBits(txnIdMostBits);
        }
        if (messageData.hasTotalChunkMsgSize() && messageData.getTotalChunkMsgSize() > 1) {
            send.setIsChunk(true);
        }

        if (messageData.hasMarkerType()) {
            send.setMarker(true);
        }

        return serializeCommandSendWithSize(cmd, checksumType, messageData, payload);
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
                startMessageRollbackDurationInSec, schemaInfo, createTopicIfDoesNotExist, null,
                Collections.emptyMap(), DEFAULT_CONSUMER_EPOCH);
    }

    public static ByteBuf newSubscribe(String topic, String subscription, long consumerId, long requestId,
               SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageIdData startMessageId,
               Map<String, String> metadata, boolean readCompacted, boolean isReplicated,
               InitialPosition subscriptionInitialPosition, long startMessageRollbackDurationInSec,
               SchemaInfo schemaInfo, boolean createTopicIfDoesNotExist, KeySharedPolicy keySharedPolicy,
               Map<String, String> subscriptionProperties, long consumerEpoch) {
        BaseCommand cmd = localCmd(Type.SUBSCRIBE);
        CommandSubscribe subscribe = cmd.setSubscribe()
                .setTopic(topic)
                .setSubscription(subscription)
                .setSubType(subType)
                .setConsumerId(consumerId)
                .setConsumerName(consumerName)
                .setRequestId(requestId)
                .setPriorityLevel(priorityLevel)
                .setDurable(isDurable)
                .setReadCompacted(readCompacted)
                .setInitialPosition(subscriptionInitialPosition)
                .setReplicateSubscriptionState(isReplicated)
                .setForceTopicCreation(createTopicIfDoesNotExist)
                .setConsumerEpoch(consumerEpoch);

        if (subscriptionProperties != null && !subscriptionProperties.isEmpty()) {
            List<KeyValue> keyValues = new ArrayList<>();
            subscriptionProperties.forEach((key, value) -> {
                KeyValue keyValue = new KeyValue();
                keyValue.setKey(key);
                keyValue.setValue(value);
                keyValues.add(keyValue);
            });
            subscribe.addAllSubscriptionProperties(keyValues);
        }

        if (keySharedPolicy != null) {
            KeySharedMeta keySharedMeta = subscribe.setKeySharedMeta();
            keySharedMeta.setAllowOutOfOrderDelivery(keySharedPolicy.isAllowOutOfOrderDelivery());
            keySharedMeta.setKeySharedMode(convertKeySharedMode(keySharedPolicy.getKeySharedMode()));

            if (keySharedPolicy instanceof KeySharedPolicy.KeySharedPolicySticky) {
                List<Range> ranges = ((KeySharedPolicy.KeySharedPolicySticky) keySharedPolicy)
                        .getRanges();
                for (Range range : ranges) {
                    IntRange r = keySharedMeta.addHashRange();
                    r.setStart(range.getStart());
                    r.setEnd(range.getEnd());
                }
            }
        }

        if (startMessageId != null) {
            subscribe.setStartMessageId().copyFrom(startMessageId);
        }
        if (startMessageRollbackDurationInSec > 0) {
            subscribe.setStartMessageRollbackDurationSec(startMessageRollbackDurationInSec);
        }

        if (!metadata.isEmpty()) {
            metadata.entrySet().forEach(e -> subscribe.addMetadata()
                    .setKey(e.getKey())
                    .setValue(e.getValue()));
        }

        if (schemaInfo != null) {
            if (subscribe.hasSchema()) {
                throw new IllegalStateException();
            }

            if (subscribe.setSchema().getPropertiesCount() > 0) {
                throw new IllegalStateException();
            }

            convertSchema(schemaInfo, subscribe.setSchema());
        }

        return serializeWithSize(cmd);
    }

    public static ByteBuf newTcClientConnectRequest(long tcId, long requestId) {
        BaseCommand cmd = localCmd(Type.TC_CLIENT_CONNECT_REQUEST);
        cmd.setTcClientConnectRequest().setTcId(tcId).setRequestId(requestId);
        return serializeWithSize(cmd);
    }

    public static BaseCommand newTcClientConnectResponse(long requestId, ServerError error, String message) {
        BaseCommand cmd = localCmd(Type.TC_CLIENT_CONNECT_RESPONSE);

        CommandTcClientConnectResponse response = cmd.setTcClientConnectResponse()
                .setRequestId(requestId);

        if (error != null) {
            response.setError(error);
        }

        if (message != null) {
            response.setMessage(message);
        }

        return cmd;
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
        BaseCommand cmd = localCmd(Type.UNSUBSCRIBE);
        cmd.setUnsubscribe()
                .setConsumerId(consumerId)
                .setRequestId(requestId);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newActiveConsumerChange(long consumerId, boolean isActive) {
        BaseCommand cmd = localCmd(Type.ACTIVE_CONSUMER_CHANGE);
        cmd.setActiveConsumerChange()
                .setConsumerId(consumerId)
                .setIsActive(isActive);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newSeek(long consumerId, long requestId,
                                  long ledgerId, long entryId, long[] ackSet) {
        BaseCommand cmd = localCmd(Type.SEEK);
        CommandSeek seek = cmd.setSeek()
                .setConsumerId(consumerId)
                .setRequestId(requestId);
        MessageIdData messageId = seek.setMessageId()
            .setLedgerId(ledgerId)
            .setEntryId(entryId);
        for (int i = 0; i < ackSet.length; i++) {
            messageId.addAckSet(ackSet[i]);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newSeek(long consumerId, long requestId, long timestamp) {
        BaseCommand cmd = localCmd(Type.SEEK);
        cmd.setSeek()
                .setConsumerId(consumerId)
                .setRequestId(requestId)
                .setMessagePublishTime(timestamp);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newCloseConsumer(long consumerId, long requestId) {
        BaseCommand cmd = localCmd(Type.CLOSE_CONSUMER);
        cmd.setCloseConsumer()
            .setConsumerId(consumerId)
            .setRequestId(requestId);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newReachedEndOfTopic(long consumerId) {
        BaseCommand cmd = localCmd(Type.REACHED_END_OF_TOPIC);
        cmd.setReachedEndOfTopic()
            .setConsumerId(consumerId);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newCloseProducer(long producerId, long requestId) {
        BaseCommand cmd = localCmd(Type.CLOSE_PRODUCER);
        cmd.setCloseProducer()
            .setProducerId(producerId)
            .setRequestId(requestId);
        return serializeWithSize(cmd);
    }

    @VisibleForTesting
    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                Map<String, String> metadata, boolean isTxnEnabled) {
        return newProducer(topic, producerId, requestId, producerName, false, metadata, isTxnEnabled);
    }

    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                boolean encrypted, Map<String, String> metadata, boolean isTxnEnabled) {
        return newProducer(topic, producerId, requestId, producerName, encrypted, metadata, null, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), isTxnEnabled);
    }

    private static Schema.Type getSchemaType(SchemaType type) {
        if (type.getValue() < 0) {
            return Schema.Type.None;
        } else {
            return Schema.Type.valueOf(type.getValue());
        }
    }

    public static SchemaType getSchemaType(Schema.Type type) {
        if (type.getValue() < 0) {
            // this is unexpected
            return SchemaType.NONE;
        } else {
            return SchemaType.valueOf(type.getValue());
        }
    }

    private static void convertSchema(SchemaInfo schemaInfo, Schema schema) {
        schema.setName(schemaInfo.getName())
                .setSchemaData(schemaInfo.getSchema())
                .setType(getSchemaType(schemaInfo.getType()));

        schemaInfo.getProperties().entrySet().stream().forEach(entry -> {
            if (entry.getKey() != null && entry.getValue() != null) {
                schema.addProperty()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue());
            }
        });
    }

    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                                      boolean encrypted, Map<String, String> metadata, SchemaInfo schemaInfo,
                                      long epoch, boolean userProvidedProducerName,
                                      ProducerAccessMode accessMode, Optional<Long> topicEpoch, boolean isTxnEnabled) {
        return newProducer(topic, producerId, requestId, producerName, encrypted, metadata, schemaInfo, epoch,
                userProvidedProducerName, accessMode, topicEpoch, isTxnEnabled, null);

    }

    public static ByteBuf newProducer(String topic, long producerId, long requestId, String producerName,
                                      boolean encrypted, Map<String, String> metadata, SchemaInfo schemaInfo,
                                      long epoch, boolean userProvidedProducerName,
                                      ProducerAccessMode accessMode, Optional<Long> topicEpoch, boolean isTxnEnabled,
                                      String initialSubscriptionName) {
        BaseCommand cmd = localCmd(Type.PRODUCER);
        CommandProducer producer = cmd.setProducer()
                .setTopic(topic)
                .setProducerId(producerId)
                .setRequestId(requestId)
                .setEpoch(epoch)
                .setUserProvidedProducerName(userProvidedProducerName)
                .setEncrypted(encrypted)
                .setTxnEnabled(isTxnEnabled)
                .setProducerAccessMode(convertProducerAccessMode(accessMode));
        if (producerName != null) {
            producer.setProducerName(producerName);
        }

        if (!metadata.isEmpty()) {
            metadata.forEach((k, v) -> producer.addMetadata()
                    .setKey(k)
                    .setValue(v));
        }

        if (null != schemaInfo) {
            convertSchema(schemaInfo, producer.setSchema());
        }

        topicEpoch.ifPresent(producer::setTopicEpoch);

        if (!Strings.isNullOrEmpty(initialSubscriptionName)) {
            producer.setInitialSubscriptionName(initialSubscriptionName);
        }

        return serializeWithSize(cmd);
    }

    public static BaseCommand newPartitionMetadataResponseCommand(ServerError error, String errorMsg, long requestId) {
        BaseCommand cmd = localCmd(Type.PARTITIONED_METADATA_RESPONSE);
        CommandPartitionedTopicMetadataResponse response = cmd.setPartitionMetadataResponse()
                .setRequestId(requestId)
                .setError(error)
                .setResponse(CommandPartitionedTopicMetadataResponse.LookupType.Failed);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }

        return cmd;
    }

    public static ByteBuf newPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {
        return serializeWithSize(newPartitionMetadataResponseCommand(error, errorMsg, requestId));
    }

    public static ByteBuf newPartitionMetadataRequest(String topic, long requestId) {
        BaseCommand cmd = localCmd(Type.PARTITIONED_METADATA);
        cmd.setPartitionMetadata()
                .setTopic(topic)
                .setRequestId(requestId);
        return serializeWithSize(cmd);
    }

    public static BaseCommand newPartitionMetadataResponseCommand(int partitions, long requestId) {
        BaseCommand cmd = localCmd(Type.PARTITIONED_METADATA_RESPONSE);
        cmd.setPartitionMetadataResponse()
                .setPartitions(partitions)
                .setResponse(CommandPartitionedTopicMetadataResponse.LookupType.Success)
                .setRequestId(requestId);
        return cmd;
    }

    public static ByteBuf newPartitionMetadataResponse(int partitions, long requestId) {
        return serializeWithSize(newPartitionMetadataResponseCommand(partitions, requestId));
    }

    public static ByteBuf newLookup(String topic, boolean authoritative, long requestId) {
        return newLookup(topic, null, authoritative, requestId);
    }

    public static ByteBuf newLookup(String topic, String listenerName, boolean authoritative, long requestId) {
        BaseCommand cmd = localCmd(Type.LOOKUP);
        CommandLookupTopic lookup = cmd.setLookupTopic()
                .setTopic(topic)
                .setRequestId(requestId)
                .setAuthoritative(authoritative);
        if (StringUtils.isNotBlank(listenerName)) {
            lookup.setAdvertisedListenerName(listenerName);
        }
        return serializeWithSize(cmd);
    }

    public static BaseCommand newLookupResponseCommand(String brokerServiceUrl, String brokerServiceUrlTls,
        boolean authoritative, LookupType lookupType, long requestId, boolean proxyThroughServiceUrl) {
        BaseCommand cmd = localCmd(Type.LOOKUP_RESPONSE);
        CommandLookupTopicResponse response = cmd.setLookupTopicResponse()
                .setResponse(lookupType)
                .setRequestId(requestId)
                .setAuthoritative(authoritative)
                .setProxyThroughServiceUrl(proxyThroughServiceUrl);
        if (brokerServiceUrl != null) {
            response.setBrokerServiceUrl(brokerServiceUrl);
        }
        if (brokerServiceUrlTls != null) {
            response.setBrokerServiceUrlTls(brokerServiceUrlTls);
        }

        return cmd;
    }

    public static ByteBuf newLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
            LookupType lookupType, long requestId, boolean proxyThroughServiceUrl) {
        return serializeWithSize(newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls, authoritative,
                lookupType, requestId, proxyThroughServiceUrl));
    }

    public static BaseCommand newLookupErrorResponseCommand(ServerError error, String errorMsg, long requestId) {
        BaseCommand cmd = localCmd(Type.LOOKUP_RESPONSE);
        CommandLookupTopicResponse response = cmd.setLookupTopicResponse()
                .setRequestId(requestId)
                .setError(error)
                .setResponse(LookupType.Failed);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }

        return cmd;
    }

    public static ByteBuf newLookupErrorResponse(ServerError error, String errorMsg, long requestId) {
        return serializeWithSize(newLookupErrorResponseCommand(error, errorMsg, requestId));
    }

    public static ByteBuf newMultiTransactionMessageAck(long consumerId, TxnID txnID,
            List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries) {
        BaseCommand cmd = newMultiMessageAckCommon(entries);
        cmd.getAck()
                .setConsumerId(consumerId)
                .setAckType(AckType.Individual)
                .setTxnidLeastBits(txnID.getLeastSigBits())
                .setTxnidMostBits(txnID.getMostSigBits());
        return serializeWithSize(cmd);
    }

    private static BaseCommand newMultiMessageAckCommon(List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries) {
        BaseCommand cmd = localCmd(Type.ACK);
        CommandAck ack = cmd.setAck();
        int entriesCount = entries.size();
        for (int i = 0; i < entriesCount; i++) {
            long ledgerId = entries.get(i).getLeft();
            long entryId = entries.get(i).getMiddle();
            ConcurrentBitSetRecyclable bitSet = entries.get(i).getRight();
            MessageIdData msgId = ack.addMessageId()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId);
            if (bitSet != null) {
                long[] ackSet = bitSet.toLongArray();
                for (int j = 0; j < ackSet.length; j++) {
                    msgId.addAckSet(ackSet[j]);
                }
                bitSet.recycle();
            }
        }

        return cmd;
    }

    public static ByteBuf newMultiMessageAck(long consumerId,
                                             List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entries,
                                             long requestId) {
        BaseCommand cmd = newMultiMessageAckCommon(entries);
        cmd.getAck()
                .setConsumerId(consumerId)
                .setAckType(AckType.Individual);
            if (requestId >= 0) {
                cmd.getAck().setRequestId(requestId);
            }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAck(long consumerId, long ledgerId, long entryId, BitSetRecyclable ackSet, AckType ackType,
                                 ValidationError validationError, Map<String, Long> properties, long requestId) {
        return newAck(consumerId, ledgerId, entryId, ackSet, ackType, validationError,
                properties, -1L, -1L, requestId, -1);
    }

    public static ByteBuf newAck(long consumerId, long ledgerId, long entryId, BitSetRecyclable ackSet, AckType ackType,
                                 ValidationError validationError, Map<String, Long> properties, long txnIdLeastBits,
                                 long txnIdMostBits, long requestId, int batchSize) {
        BaseCommand cmd = localCmd(Type.ACK);
        CommandAck ack = cmd.setAck()
                .setConsumerId(consumerId)
                .setAckType(ackType);
        MessageIdData messageIdData = ack.addMessageId()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        if (ackSet != null) {
            long[] as = ackSet.toLongArray();
            for (int i = 0; i < as.length; i++) {
                messageIdData.addAckSet(as[i]);
            }
        }

        if (batchSize >= 0) {
          messageIdData.setBatchSize(batchSize);
        }

        if (validationError != null) {
            ack.setValidationError(validationError);
        }
        if (txnIdMostBits >= 0) {
            ack.setTxnidMostBits(txnIdMostBits);
        }
        if (txnIdLeastBits >= 0) {
            ack.setTxnidLeastBits(txnIdLeastBits);
        }

        if (requestId >= 0) {
            ack.setRequestId(requestId);
        }
        if (!properties.isEmpty()) {
            properties.forEach((k, v) -> {
                ack.addProperty().setKey(k).setValue(v);
            });
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAck(long consumerId, long ledgerId, long entryId, BitSetRecyclable ackSet, AckType ackType,
                                 ValidationError validationError, Map<String, Long> properties, long txnIdLeastBits,
                                 long txnIdMostBits, long requestId) {
        return newAck(consumerId, ledgerId, entryId, ackSet, ackType, validationError,
                properties, txnIdLeastBits, txnIdMostBits, requestId, -1);
    }

    public static ByteBuf newAckResponse(long requestId, ServerError error, String errorMsg, long consumerId) {
        BaseCommand cmd = localCmd(Type.ACK_RESPONSE);
        CommandAckResponse  response = cmd.setAckResponse()
                .setConsumerId(consumerId)
                .setRequestId(requestId);

        if (error != null) {
            response.setError(error);
        }

        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }

        return serializeWithSize(cmd);
    }

    public static ByteBuf newFlow(long consumerId, int messagePermits) {
        BaseCommand cmd = localCmd(Type.FLOW);
        cmd.setFlow()
                .setConsumerId(consumerId)
                .setMessagePermits(messagePermits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newRedeliverUnacknowledgedMessages(long consumerId, long consumerEpoch) {
        BaseCommand cmd = localCmd(Type.REDELIVER_UNACKNOWLEDGED_MESSAGES);
        cmd.setRedeliverUnacknowledgedMessages()
                .setConsumerId(consumerId)
                .setConsumerEpoch(consumerEpoch);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newRedeliverUnacknowledgedMessages(long consumerId, List<MessageIdData> messageIds) {
        BaseCommand cmd = localCmd(Type.REDELIVER_UNACKNOWLEDGED_MESSAGES);
        CommandRedeliverUnacknowledgedMessages req = cmd.setRedeliverUnacknowledgedMessages()
                .setConsumerId(consumerId);
        messageIds.forEach(msgId -> {
            MessageIdData m = req.addMessageId()
                    .setLedgerId(msgId.getLedgerId())
                    .setEntryId(msgId.getEntryId());
            if (msgId.hasBatchIndex()) {
                m.setBatchIndex(msgId.getBatchIndex());
            }
        });
        return serializeWithSize(cmd);
    }

    public static ByteBuf newConsumerStatsResponse(ServerError serverError, String errMsg, long requestId) {
        return serializeWithSize(newConsumerStatsResponseCommand(serverError, errMsg, requestId));
    }

    public static BaseCommand newConsumerStatsResponseCommand(ServerError serverError, String errMsg, long requestId) {
        BaseCommand cmd = localCmd(Type.CONSUMER_STATS_RESPONSE);
        cmd.setConsumerStatsResponse()
                .setRequestId(requestId)
                .setErrorCode(serverError);
        if (errMsg != null) {
            cmd.getConsumerStatsResponse()
                .setErrorMessage(errMsg);
        }
        return cmd;
    }

    public static ByteBuf newGetTopicsOfNamespaceRequest(String namespace, long requestId, Mode mode) {
        BaseCommand cmd = localCmd(Type.GET_TOPICS_OF_NAMESPACE);
        CommandGetTopicsOfNamespace topics = cmd.setGetTopicsOfNamespace();
        topics.setNamespace(namespace);
        topics.setRequestId(requestId);
        topics.setMode(mode);
        return serializeWithSize(cmd);
    }

    public static BaseCommand newGetTopicsOfNamespaceResponseCommand(List<String> topics, long requestId) {
        BaseCommand cmd = localCmd(Type.GET_TOPICS_OF_NAMESPACE_RESPONSE);
        CommandGetTopicsOfNamespaceResponse topicsResponse = cmd.setGetTopicsOfNamespaceResponse();
        topicsResponse.setRequestId(requestId);
        for (int i = 0; i < topics.size(); i++) {
            topicsResponse.addTopic(topics.get(i));
        }

        return cmd;
    }

    public static ByteBuf newGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        return serializeWithSize(newGetTopicsOfNamespaceResponseCommand(topics, requestId));
    }

    private static final ByteBuf cmdPing;

    static {
        BaseCommand cmd = new BaseCommand()
                .setType(Type.PING);
        cmd.setPing();
        ByteBuf serializedCmdPing = serializeWithSize(cmd);
        cmdPing = Unpooled.copiedBuffer(serializedCmdPing);
        serializedCmdPing.release();
    }

    static ByteBuf newPing() {
        return cmdPing.retainedDuplicate();
    }

    private static final ByteBuf cmdPong;

    static {
        BaseCommand cmd = new BaseCommand()
                .setType(Type.PONG);
        cmd.setPong();
        ByteBuf serializedCmdPong = serializeWithSize(cmd);
        cmdPong = Unpooled.copiedBuffer(serializedCmdPong);
        serializedCmdPong.release();
    }

    public static ByteBuf newPong() {
        return cmdPong.retainedDuplicate();
    }

    public static ByteBuf newGetLastMessageId(long consumerId, long requestId) {
        BaseCommand cmd = localCmd(Type.GET_LAST_MESSAGE_ID);
        cmd.setGetLastMessageId()
                .setRequestId(requestId)
                .setConsumerId(consumerId);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newGetLastMessageIdResponse(long requestId,
            long lastMessageLedgerId, long lastMessageEntryId,
            int lastMessagePartitionIdx, int lastMessageBatchIndex,
            long markDeletePositionLedgerId, long markDeletePositionEntryId) {
        BaseCommand cmd = localCmd(Type.GET_LAST_MESSAGE_ID_RESPONSE);
        CommandGetLastMessageIdResponse response = cmd.setGetLastMessageIdResponse()
            .setRequestId(requestId);

        response.setLastMessageId()
            .setLedgerId(lastMessageLedgerId)
            .setEntryId(lastMessageEntryId)
            .setPartition(lastMessagePartitionIdx)
            .setBatchIndex(lastMessageBatchIndex);

        if (markDeletePositionLedgerId >= 0) {
            response.setConsumerMarkDeletePosition()
                    .setLedgerId(markDeletePositionLedgerId)
                    .setEntryId(markDeletePositionEntryId);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newGetSchema(long requestId, String topic, Optional<SchemaVersion> version) {
        BaseCommand cmd = localCmd(Type.GET_SCHEMA);
        CommandGetSchema schema = cmd.setGetSchema()
                .setRequestId(requestId)
                .setTopic(topic);
        version.ifPresent(schemaVersion -> schema.setSchemaVersion(schemaVersion.bytes()));
        return serializeWithSize(cmd);
    }

    public static ByteBuf newGetSchemaResponse(long requestId, CommandGetSchemaResponse response) {
        BaseCommand cmd = localCmd(Type.GET_SCHEMA_RESPONSE);
        cmd.setGetSchemaResponse()
            .copyFrom(response)
            .setRequestId(requestId);

        return serializeWithSize(cmd);
    }

    public static BaseCommand newGetSchemaResponseCommand(long requestId,
            SchemaInfo schemaInfo, SchemaVersion version) {
        BaseCommand cmd = localCmd(Type.GET_SCHEMA_RESPONSE);
        Schema schema = cmd.setGetSchemaResponse()
                .setRequestId(requestId)
                .setSchemaVersion(version.bytes())
                .setSchema();
        convertSchema(schemaInfo, schema);
        return cmd;
    }

    public static ByteBuf newGetSchemaResponse(long requestId, SchemaInfo schemaInfo, SchemaVersion version) {
        return serializeWithSize(newGetSchemaResponseCommand(requestId, schemaInfo, version));
    }

    public static BaseCommand newGetSchemaResponseErrorCommand(long requestId, ServerError error, String errorMessage) {
        BaseCommand cmd = localCmd(Type.GET_SCHEMA_RESPONSE);
        cmd.setGetSchemaResponse()
                .setRequestId(requestId)
                .setErrorCode(error)
                .setErrorMessage(errorMessage);
        return cmd;
    }

    public static ByteBuf newGetSchemaResponseError(long requestId, ServerError error, String errorMessage) {
        return serializeWithSize(newGetSchemaResponseErrorCommand(requestId, error, errorMessage));
    }

    public static ByteBuf newGetOrCreateSchema(long requestId, String topic, SchemaInfo schemaInfo) {
        BaseCommand cmd = localCmd(Type.GET_OR_CREATE_SCHEMA);
        Schema schema = cmd.setGetOrCreateSchema()
                .setRequestId(requestId)
                .setTopic(topic)
                .setSchema();
        convertSchema(schemaInfo, schema);
        return serializeWithSize(cmd);
    }

    public static BaseCommand newGetOrCreateSchemaResponseCommand(long requestId, SchemaVersion schemaVersion) {
        BaseCommand cmd = localCmd(Type.GET_OR_CREATE_SCHEMA_RESPONSE);
        cmd.setGetOrCreateSchemaResponse()
                .setRequestId(requestId)
                .setSchemaVersion(schemaVersion.bytes());
        return cmd;
    }

    public static ByteBuf newGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        return serializeWithSize(newGetOrCreateSchemaResponseCommand(requestId, schemaVersion));
    }

    public static BaseCommand newGetOrCreateSchemaResponseErrorCommand(long requestId, ServerError error,
            String errorMessage) {
        BaseCommand cmd = localCmd(Type.GET_OR_CREATE_SCHEMA_RESPONSE);
        cmd.setGetOrCreateSchemaResponse()
                .setRequestId(requestId)
                .setErrorCode(error)
                .setErrorMessage(errorMessage);
        return cmd;
    }

    public static ByteBuf newGetOrCreateSchemaResponseError(long requestId, ServerError error, String errorMessage) {
        BaseCommand cmd = localCmd(Type.GET_OR_CREATE_SCHEMA_RESPONSE);
        cmd.setGetOrCreateSchemaResponse()
                .setRequestId(requestId)
                .setErrorCode(error)
                .setErrorMessage(errorMessage);
        return serializeWithSize(cmd);
    }

    // ---- transaction related ----

    public static ByteBuf newTxn(long tcId, long requestId, long ttlSeconds) {
        BaseCommand cmd = localCmd(Type.NEW_TXN);
        cmd.setNewTxn()
                .setTcId(tcId)
                .setRequestId(requestId)
                .setTxnTtlSeconds(ttlSeconds);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.NEW_TXN_RESPONSE);
        cmd.setNewTxnResponse()
                .setRequestId(requestId)
                .setTxnidMostBits(txnIdMostBits)
                .setTxnidLeastBits(txnIdLeastBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newTxnResponse(long requestId, long txnIdMostBits, ServerError error, String errorMsg) {
        BaseCommand cmd = localCmd(Type.NEW_TXN_RESPONSE);
        CommandNewTxnResponse response = cmd.setNewTxnResponse()
                .setRequestId(requestId)
                .setTxnidMostBits(txnIdMostBits)
                .setError(error);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddPartitionToTxn(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                               List<String> partitions) {
        BaseCommand cmd = localCmd(Type.ADD_PARTITION_TO_TXN);
        CommandAddPartitionToTxn req = cmd.setAddPartitionToTxn()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        if (partitions != null) {
            partitions.forEach(req::addPartition);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddPartitionToTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.ADD_PARTITION_TO_TXN_RESPONSE);
        cmd.setAddPartitionToTxnResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddPartitionToTxnResponse(long requestId, long txnIdMostBits, ServerError error,
           String errorMsg) {
        BaseCommand cmd = localCmd(Type.ADD_PARTITION_TO_TXN_RESPONSE);
        CommandAddPartitionToTxnResponse response = cmd.setAddPartitionToTxnResponse()
                .setRequestId(requestId)
                .setError(error)
                .setTxnidMostBits(txnIdMostBits);

        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddSubscriptionToTxn(long requestId, long txnIdLeastBits, long txnIdMostBits,
            List<Subscription> subscriptions) {
        BaseCommand cmd = localCmd(Type.ADD_SUBSCRIPTION_TO_TXN);
        CommandAddSubscriptionToTxn add = cmd.setAddSubscriptionToTxn()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        subscriptions.forEach(s -> add.addSubscription().copyFrom(s));
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddSubscriptionToTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.ADD_SUBSCRIPTION_TO_TXN_RESPONSE);
        cmd.setAddSubscriptionToTxnResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newAddSubscriptionToTxnResponse(long requestId, long txnIdMostBits, ServerError error,
          String errorMsg) {
        BaseCommand cmd = localCmd(Type.ADD_SUBSCRIPTION_TO_TXN_RESPONSE);
        CommandAddSubscriptionToTxnResponse response = cmd.setAddSubscriptionToTxnResponse()
                .setRequestId(requestId)
                .setTxnidMostBits(txnIdMostBits)
                .setError(error);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static BaseCommand newEndTxn(long requestId, long txnIdLeastBits, long txnIdMostBits, TxnAction txnAction) {
        BaseCommand cmd = localCmd(Type.END_TXN);
        cmd.setEndTxn()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits).setTxnidMostBits(txnIdMostBits)
                .setTxnAction(txnAction);
        return cmd;
    }

    public static ByteBuf newEndTxnResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.END_TXN_RESPONSE);
        cmd.setEndTxnResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnResponse(long requestId, long txnIdMostBits, ServerError error, String errorMsg) {
        BaseCommand cmd = localCmd(Type.END_TXN_RESPONSE);
        CommandEndTxnResponse response = cmd.setEndTxnResponse()
                .setRequestId(requestId)
                .setTxnidMostBits(txnIdMostBits)
                .setError(error);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnPartition(long requestId, long txnIdLeastBits, long txnIdMostBits, String topic,
                                               TxnAction txnAction, long lowWaterMark) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_PARTITION);
        cmd.setEndTxnOnPartition()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setTopic(topic)
                .setTxnAction(txnAction)
                .setTxnidLeastBitsOfLowWatermark(lowWaterMark);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnPartitionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_PARTITION_RESPONSE);
        cmd.setEndTxnOnPartitionResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnPartitionResponse(long requestId, ServerError error, String errorMsg,
                                                       long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_PARTITION_RESPONSE);
        CommandEndTxnOnPartitionResponse response = cmd.setEndTxnOnPartitionResponse()
                .setRequestId(requestId)
                .setTxnidMostBits(txnIdMostBits)
                .setTxnidLeastBits(txnIdLeastBits)
                .setError(error);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnSubscription(long requestId, long txnIdLeastBits, long txnIdMostBits, String topic,
            String subscription, TxnAction txnAction, long lowWaterMark) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_SUBSCRIPTION);
        cmd.setEndTxnOnSubscription()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setTxnAction(txnAction)
                .setTxnidLeastBitsOfLowWatermark(lowWaterMark)
                .setSubscription()
                .setTopic(topic)
                .setSubscription(subscription);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnSubscriptionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_SUBSCRIPTION_RESPONSE);
        cmd.setEndTxnOnSubscriptionResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits);
        return serializeWithSize(cmd);
    }

    public static ByteBuf newEndTxnOnSubscriptionResponse(long requestId, long txnIdLeastBits, long txnIdMostBits,
                                                          ServerError error, String errorMsg) {
        BaseCommand cmd = localCmd(Type.END_TXN_ON_SUBSCRIPTION_RESPONSE);
        CommandEndTxnOnSubscriptionResponse response = cmd.setEndTxnOnSubscriptionResponse()
                .setRequestId(requestId)
                .setTxnidLeastBits(txnIdLeastBits)
                .setTxnidMostBits(txnIdMostBits)
                .setError(error);
        if (errorMsg != null) {
            response.setMessage(errorMsg);
        }
        return serializeWithSize(cmd);
    }

    public static ByteBuf serializeWithSize(BaseCommand cmd) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD]
        int cmdSize = cmd.getSerializedSize();
        int totalSize = cmdSize + 4;
        int frameSize = totalSize + 4;

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(frameSize, frameSize);

        // Prepend 2 lengths to the buffer
        buf.writeInt(totalSize);
        buf.writeInt(cmdSize);
        cmd.writeTo(buf);
        return buf;
    }

    private static ByteBufPair serializeCommandSendWithSize(BaseCommand cmd, ChecksumType checksumType,
            MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]

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

        // Write cmd
        headers.writeInt(cmdSize);
        cmd.writeTo(headers);

        // Create checksum placeholder
        if (includeChecksum) {
            headers.writeShort(magicCrc32c);
            checksumReaderIndex = headers.writerIndex();
            headers.writerIndex(headers.writerIndex() + checksumSize); // skip 4 bytes of checksum
        }

        // Write metadata
        headers.writeInt(msgMetadataSize);
        msgMetadata.writeTo(headers);

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

    public static ByteBuf addBrokerEntryMetadata(ByteBuf headerAndPayload,
                                                 Set<BrokerEntryMetadataInterceptor> interceptors) {
        return addBrokerEntryMetadata(headerAndPayload, interceptors, -1);
    }

    public static ByteBuf addBrokerEntryMetadata(ByteBuf headerAndPayload,
                                                 Set<BrokerEntryMetadataInterceptor> brokerInterceptors,
                                                 int numberOfMessages) {
        //   | BROKER_ENTRY_METADATA_MAGIC_NUMBER | BROKER_ENTRY_METADATA_SIZE |         BROKER_ENTRY_METADATA         |
        //   |         2 bytes                    |       4 bytes              |    BROKER_ENTRY_METADATA_SIZE bytes   |

        BrokerEntryMetadata brokerEntryMetadata = BROKER_ENTRY_METADATA.get();
        for (BrokerEntryMetadataInterceptor interceptor : brokerInterceptors) {
            interceptor.intercept(brokerEntryMetadata);
            if (numberOfMessages >= 0) {
                interceptor.interceptWithNumberOfMessages(brokerEntryMetadata, numberOfMessages);
            }
        }

        int brokerMetaSize = brokerEntryMetadata.getSerializedSize();
        ByteBuf brokerMeta =
                PulsarByteBufAllocator.DEFAULT.buffer(brokerMetaSize + 6, brokerMetaSize + 6);
        brokerMeta.writeShort(Commands.magicBrokerEntryMetadata);
        brokerMeta.writeInt(brokerMetaSize);
        brokerEntryMetadata.writeTo(brokerMeta);

        CompositeByteBuf compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        compositeByteBuf.addComponents(true, brokerMeta, headerAndPayload);
        return compositeByteBuf;
    }

    public static ByteBuf skipBrokerEntryMetadataIfExist(ByteBuf headerAndPayloadWithBrokerEntryMetadata) {
        int readerIndex = headerAndPayloadWithBrokerEntryMetadata.readerIndex();
        if (headerAndPayloadWithBrokerEntryMetadata.readShort() == magicBrokerEntryMetadata) {
            int brokerEntryMetadataSize = headerAndPayloadWithBrokerEntryMetadata.readInt();
            headerAndPayloadWithBrokerEntryMetadata.readerIndex(headerAndPayloadWithBrokerEntryMetadata.readerIndex()
                    + brokerEntryMetadataSize);
        } else {
            headerAndPayloadWithBrokerEntryMetadata.readerIndex(readerIndex);
        }
        return headerAndPayloadWithBrokerEntryMetadata;
    }

    public static BrokerEntryMetadata parseBrokerEntryMetadataIfExist(
            ByteBuf headerAndPayloadWithBrokerEntryMetadata) {
        int readerIndex = headerAndPayloadWithBrokerEntryMetadata.readerIndex();
        if (headerAndPayloadWithBrokerEntryMetadata.getShort(readerIndex) == magicBrokerEntryMetadata) {
            headerAndPayloadWithBrokerEntryMetadata.skipBytes(2);
            int brokerEntryMetadataSize = headerAndPayloadWithBrokerEntryMetadata.readInt();
            BrokerEntryMetadata brokerEntryMetadata = new BrokerEntryMetadata();
            brokerEntryMetadata.parseFrom(headerAndPayloadWithBrokerEntryMetadata, brokerEntryMetadataSize);
            return brokerEntryMetadata;
        } else {
            return null;
        }
    }

    public static BrokerEntryMetadata peekBrokerEntryMetadataIfExist(
            ByteBuf headerAndPayloadWithBrokerEntryMetadata) {
        final int readerIndex = headerAndPayloadWithBrokerEntryMetadata.readerIndex();
        BrokerEntryMetadata entryMetadata =
                parseBrokerEntryMetadataIfExist(headerAndPayloadWithBrokerEntryMetadata);
        headerAndPayloadWithBrokerEntryMetadata.readerIndex(readerIndex);
        return entryMetadata;
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

        // Create checksum placeholder
        if (includeChecksum) {
            metadataAndPayload.writeShort(magicCrc32c);
            checksumReaderIndex = metadataAndPayload.writerIndex();
            metadataAndPayload.writerIndex(metadataAndPayload.writerIndex()
                    + checksumSize); // skip 4 bytes of checksum
        }

        // Write metadata
        metadataAndPayload.writeInt(msgMetadataSize);
        msgMetadata.writeTo(metadataAndPayload);

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

    public static long initBatchMessageMetadata(MessageMetadata messageMetadata,
            MessageMetadata builder) {
        messageMetadata.setPublishTime(builder.getPublishTime());
        messageMetadata.setProducerName(builder.getProducerName());
        messageMetadata.setSequenceId(builder.getSequenceId());

        // Attach the key to the message metadata.
        if (builder.hasPartitionKey()) {
            messageMetadata.setPartitionKey(builder.getPartitionKey());
            messageMetadata.setPartitionKeyB64Encoded(builder.isPartitionKeyB64Encoded());
        }
        if (builder.hasOrderingKey()) {
            messageMetadata.setOrderingKey(builder.getOrderingKey());
        }
        if (builder.hasReplicatedFrom()) {
            messageMetadata.setReplicatedFrom(builder.getReplicatedFrom());
        }
        if (builder.getReplicateTosCount() > 0) {
            for (int i = 0; i < builder.getReplicateTosCount(); i++) {
                messageMetadata.addReplicateTo(builder.getReplicateToAt(i));
            }
        }
        if (builder.hasSchemaVersion()) {
            messageMetadata.setSchemaVersion(builder.getSchemaVersion());
        }

        return builder.getSequenceId();
    }

    public static ByteBuf serializeSingleMessageInBatchWithPayload(
            SingleMessageMetadata singleMessageMetadata,
            ByteBuf payload, ByteBuf batchBuffer) {
        singleMessageMetadata.setPayloadSize(payload.readableBytes());

        // serialize meta-data size, meta-data and payload for single message in batch
        batchBuffer.writeInt(singleMessageMetadata.getSerializedSize());
        singleMessageMetadata.writeTo(batchBuffer);
        return batchBuffer.writeBytes(payload);
    }

    public static ByteBuf serializeSingleMessageInBatchWithPayload(MessageMetadata msg,
            ByteBuf payload, ByteBuf batchBuffer) {
        // build single message meta-data
        SingleMessageMetadata smm = LOCAL_SINGLE_MESSAGE_METADATA.get();
        smm.clear();

        if (msg.hasPartitionKey()) {
            smm.setPartitionKey(msg.getPartitionKey());
            smm.setPartitionKeyB64Encoded(msg.isPartitionKeyB64Encoded());
        }
        if (msg.hasOrderingKey()) {
            smm.setOrderingKey(msg.getOrderingKey());
        }
        for (int i = 0; i < msg.getPropertiesCount(); i++) {
            smm.addProperty()
                    .setKey(msg.getPropertyAt(i).getKey())
                    .setValue(msg.getPropertyAt(i).getValue());
        }

        if (msg.hasEventTime()) {
            smm.setEventTime(msg.getEventTime());
        }

        if (msg.hasSequenceId()) {
            smm.setSequenceId(msg.getSequenceId());
        }

        if (msg.hasNullValue()) {
            smm.setNullValue(msg.isNullValue());
        }

        if (msg.hasNullPartitionKey()) {
            smm.setNullPartitionKey(msg.isNullPartitionKey());
        }

        return serializeSingleMessageInBatchWithPayload(smm, payload, batchBuffer);
    }

    public static ByteBuf deSerializeSingleMessageInBatch(ByteBuf uncompressedPayload,
            SingleMessageMetadata singleMessageMetadata, int index, int batchSize)
            throws IOException {
        int singleMetaSize = (int) uncompressedPayload.readUnsignedInt();
        singleMessageMetadata.parseFrom(uncompressedPayload, singleMetaSize);

        int singleMessagePayloadSize = singleMessageMetadata.getPayloadSize();

        int readerIndex = uncompressedPayload.readerIndex();
        ByteBuf singleMessagePayload = uncompressedPayload.retainedSlice(readerIndex, singleMessagePayloadSize);

        // reader now points to beginning of payload read; so move it past message payload just read
        if (index < batchSize) {
            uncompressedPayload.readerIndex(readerIndex + singleMessagePayloadSize);
        }

        return singleMessagePayload;
    }

    public static ByteBufPair serializeCommandMessageWithSize(BaseCommand cmd, ByteBuf metadataAndPayload) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        //
        // metadataAndPayload contains from magic-number to the payload included

        int cmdSize = cmd.getSerializedSize();
        int totalSize = 4 + cmdSize + metadataAndPayload.readableBytes();
        int headersSize = 4 + 4 + cmdSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.buffer(headersSize);
        headers.writeInt(totalSize); // External frame

        // Write cmd
        headers.writeInt(cmdSize);
        cmd.writeTo(headers);
        return ByteBufPair.get(headers, metadataAndPayload);
    }

    public static int getNumberOfMessagesInBatch(ByteBuf metadataAndPayload, String subscription,
            long consumerId) {
        MessageMetadata msgMetadata = peekMessageMetadata(metadataAndPayload, subscription, consumerId);
        if (msgMetadata == null) {
            return -1;
        } else {
            return msgMetadata.getNumMessagesInBatch();
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

    private static final byte[] NONE_KEY = "NONE_KEY".getBytes(StandardCharsets.UTF_8);
    public static byte[] peekStickyKey(ByteBuf metadataAndPayload, String topic, String subscription) {
        try {
            int readerIdx = metadataAndPayload.readerIndex();
            MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);
            metadataAndPayload.readerIndex(readerIdx);
            if (metadata.hasOrderingKey()) {
                return metadata.getOrderingKey();
            } else if (metadata.hasPartitionKey()) {
                if (metadata.isPartitionKeyB64Encoded()) {
                    return Base64.getDecoder().decode(metadata.getPartitionKey());
                }
                return metadata.getPartitionKey().getBytes(StandardCharsets.UTF_8);
            }
        } catch (Throwable t) {
            log.error("[{}] [{}] Failed to peek sticky key from the message metadata", topic, subscription, t);
        }
        return Commands.NONE_KEY;
    }

    public static int getCurrentProtocolVersion() {
        return CURRENT_PROTOCOL_VERSION;
    }

    /**
     * Definition of possible checksum types.
     */
    public enum ChecksumType {
        Crc32c,
        None;
    }

    public static boolean peerSupportsGetLastMessageId(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getValue();
    }

    public static boolean peerSupportsActiveConsumerListener(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getValue();
    }

    public static boolean peerSupportsMultiMessageAcknowledgment(int peerVersion) {
        return peerVersion >= ProtocolVersion.v12.getValue();
    }

    public static boolean peerSupportJsonSchemaAvroFormat(int peerVersion) {
        return peerVersion >= ProtocolVersion.v13.getValue();
    }

    public static boolean peerSupportsGetOrCreateSchema(int peerVersion) {
        return peerVersion >= ProtocolVersion.v15.getValue();
    }

    public static boolean peerSupportsAckReceipt(int peerVersion) {
        return peerVersion >= ProtocolVersion.v17.getValue();
    }

    private static org.apache.pulsar.common.api.proto.ProducerAccessMode convertProducerAccessMode(
            ProducerAccessMode accessMode) {
        switch (accessMode) {
        case Exclusive:
            return org.apache.pulsar.common.api.proto.ProducerAccessMode.Exclusive;
        case Shared:
            return org.apache.pulsar.common.api.proto.ProducerAccessMode.Shared;
        case WaitForExclusive:
            return org.apache.pulsar.common.api.proto.ProducerAccessMode.WaitForExclusive;
        default:
            throw new IllegalArgumentException("Unknonw access mode: " + accessMode);
        }
    }

    public static ProducerAccessMode convertProducerAccessMode(
            org.apache.pulsar.common.api.proto.ProducerAccessMode accessMode) {
        switch (accessMode) {
        case Exclusive:
            return ProducerAccessMode.Exclusive;
        case Shared:
            return ProducerAccessMode.Shared;
        case WaitForExclusive:
            return ProducerAccessMode.WaitForExclusive;
        default:
            throw new IllegalArgumentException("Unknonw access mode: " + accessMode);
        }
    }

    public static boolean peerSupportsBrokerMetadata(int peerVersion) {
        return peerVersion >= ProtocolVersion.v16.getValue();
    }
}
