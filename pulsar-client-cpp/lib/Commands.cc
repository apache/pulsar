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
#include "Commands.h"
#include "MessageImpl.h"
#include "Version.h"
#include "pulsar/MessageBuilder.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"
#include "Utils.h"
#include "Url.h"
#include <pulsar/Schema.h>
#include "checksum/ChecksumProvider.h"
#include <algorithm>
#include <mutex>

using namespace pulsar;
namespace pulsar {

using namespace pulsar::proto;

DECLARE_LOG_OBJECT();

static inline bool isBuiltInSchema(SchemaType schemaType) {
    switch (schemaType) {
        case STRING:
        case JSON:
        case AVRO:
        case PROTOBUF:
        case PROTOBUF_NATIVE:
            return true;

        default:
            return false;
    }
}

static inline proto::Schema_Type getSchemaType(SchemaType type) {
    switch (type) {
        case SchemaType::NONE:
            return Schema_Type_None;
        case STRING:
            return Schema_Type_String;
        case JSON:
            return Schema_Type_Json;
        case PROTOBUF:
            return Schema_Type_Protobuf;
        case AVRO:
            return Schema_Type_Avro;
        case PROTOBUF_NATIVE:
            return Schema_Type_ProtobufNative;
        default:
            return Schema_Type_None;
    }
}

static proto::Schema* getSchema(const SchemaInfo& schemaInfo) {
    proto::Schema* schema = proto::Schema().New();
    schema->set_name(schemaInfo.getName());
    schema->set_schema_data(schemaInfo.getSchema());
    schema->set_type(getSchemaType(schemaInfo.getSchemaType()));
    for (const auto& kv : schemaInfo.getProperties()) {
        proto::KeyValue* keyValue = proto::KeyValue().New();
        keyValue->set_key(kv.first);
        keyValue->set_value(kv.second);
        schema->mutable_properties()->AddAllocated(keyValue);
    }

    return schema;
}

SharedBuffer Commands::writeMessageWithSize(const BaseCommand& cmd) {
    size_t cmdSize = cmd.ByteSize();
    size_t frameSize = 4 + cmdSize;
    size_t bufferSize = 4 + frameSize;

    SharedBuffer buffer = SharedBuffer::allocate(bufferSize);

    buffer.writeUnsignedInt(frameSize);
    buffer.writeUnsignedInt(cmdSize);
    cmd.SerializeToArray(buffer.mutableData(), cmdSize);
    buffer.bytesWritten(cmdSize);
    return buffer;
}

SharedBuffer Commands::newPartitionMetadataRequest(const std::string& topic, uint64_t requestId) {
    static BaseCommand cmd;
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    cmd.set_type(BaseCommand::PARTITIONED_METADATA);
    CommandPartitionedTopicMetadata* partitionMetadata = cmd.mutable_partitionmetadata();
    partitionMetadata->set_topic(topic);
    partitionMetadata->set_request_id(requestId);
    const SharedBuffer buffer = writeMessageWithSize(cmd);
    cmd.clear_partitionmetadata();
    return buffer;
}

SharedBuffer Commands::newLookup(const std::string& topic, const bool authoritative, uint64_t requestId,
                                 const std::string& listenerName) {
    static BaseCommand cmd;
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    cmd.set_type(BaseCommand::LOOKUP);
    CommandLookupTopic* lookup = cmd.mutable_lookuptopic();
    lookup->set_topic(topic);
    lookup->set_authoritative(authoritative);
    lookup->set_request_id(requestId);
    lookup->set_advertised_listener_name(listenerName);
    const SharedBuffer buffer = writeMessageWithSize(cmd);
    cmd.clear_lookuptopic();
    return buffer;
}

SharedBuffer Commands::newConsumerStats(uint64_t consumerId, uint64_t requestId) {
    static BaseCommand cmd;
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    cmd.set_type(BaseCommand::CONSUMER_STATS);
    CommandConsumerStats* consumerStats = cmd.mutable_consumerstats();
    consumerStats->set_consumer_id(consumerId);
    consumerStats->set_request_id(requestId);
    const SharedBuffer buffer = writeMessageWithSize(cmd);
    cmd.clear_consumerstats();
    return buffer;
}

PairSharedBuffer Commands::newSend(SharedBuffer& headers, BaseCommand& cmd, uint64_t producerId,
                                   uint64_t sequenceId, ChecksumType checksumType, const Message& msg) {
    const proto::MessageMetadata& metadata = msg.impl_->metadata;
    SharedBuffer& payload = msg.impl_->payload;

    cmd.set_type(BaseCommand::SEND);
    CommandSend* send = cmd.mutable_send();
    send->set_producer_id(producerId);
    send->set_sequence_id(sequenceId);
    if (metadata.has_num_messages_in_batch()) {
        send->set_num_messages(metadata.num_messages_in_batch());
    }

    // / Wire format
    // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]

    int cmdSize = cmd.ByteSize();
    int msgMetadataSize = metadata.ByteSize();
    int payloadSize = payload.readableBytes();

    int magicAndChecksumLength = (Crc32c == (checksumType)) ? (2 + 4 /* magic + checksumLength*/) : 0;
    bool includeChecksum = magicAndChecksumLength > 0;
    int headerContentSize =
        4 + cmdSize + magicAndChecksumLength + 4 + msgMetadataSize;  // cmdLength + cmdSize + magicLength +
    // checksumSize + msgMetadataLength + msgMetadataSize
    int totalSize = headerContentSize + payloadSize;
    int checksumReaderIndex = -1;

    headers.reset();
    assert(headers.writableBytes() >= (4 + headerContentSize));  // totalSize + headerLength
    headers.writeUnsignedInt(totalSize);                         // External frame

    // Write cmd
    headers.writeUnsignedInt(cmdSize);
    cmd.SerializeToArray(headers.mutableData(), cmdSize);
    headers.bytesWritten(cmdSize);

    // Create checksum placeholder
    if (includeChecksum) {
        headers.writeUnsignedShort(magicCrc32c);
        checksumReaderIndex = headers.writerIndex();
        headers.skipBytes(checksumSize);  // skip 4 bytes of checksum
    }

    // Write metadata
    headers.writeUnsignedInt(msgMetadataSize);
    metadata.SerializeToArray(headers.mutableData(), msgMetadataSize);
    headers.bytesWritten(msgMetadataSize);

    PairSharedBuffer composite;
    composite.set(0, headers);
    composite.set(1, payload);

    // Write checksum at created checksum-placeholder
    if (includeChecksum) {
        int writeIndex = headers.writerIndex();
        int metadataStartIndex = checksumReaderIndex + checksumSize;
        uint32_t metadataChecksum =
            computeChecksum(0, headers.data() + metadataStartIndex, (writeIndex - metadataStartIndex));
        uint32_t computedChecksum = computeChecksum(metadataChecksum, payload.data(), payload.writerIndex());
        // set computed checksum
        headers.setWriterIndex(checksumReaderIndex);
        headers.writeUnsignedInt(computedChecksum);
        headers.setWriterIndex(writeIndex);
    }

    cmd.clear_send();
    return composite;
}

SharedBuffer Commands::newConnect(const AuthenticationPtr& authentication, const std::string& logicalAddress,
                                  bool connectingThroughProxy, Result& result) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::CONNECT);
    CommandConnect* connect = cmd.mutable_connect();
    connect->set_client_version(_PULSAR_VERSION_);
    connect->set_auth_method_name(authentication->getAuthMethodName());
    connect->set_protocol_version(ProtocolVersion_MAX);

    FeatureFlags* flags = connect->mutable_feature_flags();
    flags->set_supports_auth_refresh(true);
    if (connectingThroughProxy) {
        Url logicalAddressUrl;
        Url::parse(logicalAddress, logicalAddressUrl);
        connect->set_proxy_to_broker_url(logicalAddressUrl.hostPort());
    }

    AuthenticationDataPtr authDataContent;
    result = authentication->getAuthData(authDataContent);
    if (result != ResultOk) {
        return SharedBuffer{};
    }

    if (authDataContent->hasDataFromCommand()) {
        connect->set_auth_data(authDataContent->getCommandData());
    }
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newAuthResponse(const AuthenticationPtr& authentication, Result& result) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::AUTH_RESPONSE);
    CommandAuthResponse* authResponse = cmd.mutable_authresponse();
    authResponse->set_client_version(_PULSAR_VERSION_);

    AuthData* authData = authResponse->mutable_response();
    authData->set_auth_method_name(authentication->getAuthMethodName());

    AuthenticationDataPtr authDataContent;
    result = authentication->getAuthData(authDataContent);
    if (result != ResultOk) {
        return SharedBuffer{};
    }

    if (authDataContent->hasDataFromCommand()) {
        authData->set_auth_data(authDataContent->getCommandData());
    }

    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newSubscribe(const std::string& topic, const std::string& subscription,
                                    uint64_t consumerId, uint64_t requestId, CommandSubscribe_SubType subType,
                                    const std::string& consumerName, SubscriptionMode subscriptionMode,
                                    Optional<MessageId> startMessageId, bool readCompacted,
                                    const std::map<std::string, std::string>& metadata,
                                    const SchemaInfo& schemaInfo,
                                    CommandSubscribe_InitialPosition subscriptionInitialPosition,
                                    bool replicateSubscriptionState, KeySharedPolicy keySharedPolicy,
                                    int priorityLevel) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::SUBSCRIBE);
    CommandSubscribe* subscribe = cmd.mutable_subscribe();
    subscribe->set_topic(topic);
    subscribe->set_subscription(subscription);
    subscribe->set_subtype(subType);
    subscribe->set_consumer_id(consumerId);
    subscribe->set_request_id(requestId);
    subscribe->set_consumer_name(consumerName);
    subscribe->set_durable(subscriptionMode == SubscriptionModeDurable);
    subscribe->set_read_compacted(readCompacted);
    subscribe->set_initialposition(subscriptionInitialPosition);
    subscribe->set_replicate_subscription_state(replicateSubscriptionState);
    subscribe->set_priority_level(priorityLevel);

    if (isBuiltInSchema(schemaInfo.getSchemaType())) {
        subscribe->set_allocated_schema(getSchema(schemaInfo));
    }

    if (startMessageId.is_present()) {
        MessageIdData& messageIdData = *subscribe->mutable_start_message_id();
        messageIdData.set_ledgerid(startMessageId.value().ledgerId());
        messageIdData.set_entryid(startMessageId.value().entryId());

        if (startMessageId.value().batchIndex() != -1) {
            messageIdData.set_batch_index(startMessageId.value().batchIndex());
        }
    }
    for (std::map<std::string, std::string>::const_iterator it = metadata.begin(); it != metadata.end();
         it++) {
        proto::KeyValue* keyValue = proto::KeyValue().New();
        keyValue->set_key(it->first);
        keyValue->set_value(it->second);
        subscribe->mutable_metadata()->AddAllocated(keyValue);
    }

    if (subType == CommandSubscribe_SubType_Key_Shared) {
        KeySharedMeta& ksm = *subscribe->mutable_keysharedmeta();
        switch (keySharedPolicy.getKeySharedMode()) {
            case pulsar::AUTO_SPLIT:
                ksm.set_keysharedmode(proto::KeySharedMode::AUTO_SPLIT);
                break;
            case pulsar::STICKY:
                ksm.set_keysharedmode(proto::KeySharedMode::STICKY);
                for (StickyRange range : keySharedPolicy.getStickyRanges()) {
                    IntRange* intRange = IntRange().New();
                    intRange->set_start(range.first);
                    intRange->set_end(range.second);
                    ksm.mutable_hashranges()->AddAllocated(intRange);
                }
        }
        ksm.set_allowoutoforderdelivery(keySharedPolicy.isAllowOutOfOrderDelivery());
    }

    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newUnsubscribe(uint64_t consumerId, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::UNSUBSCRIBE);
    CommandUnsubscribe* unsubscribe = cmd.mutable_unsubscribe();
    unsubscribe->set_consumer_id(consumerId);
    unsubscribe->set_request_id(requestId);

    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newProducer(const std::string& topic, uint64_t producerId,
                                   const std::string& producerName, uint64_t requestId,
                                   const std::map<std::string, std::string>& metadata,
                                   const SchemaInfo& schemaInfo, uint64_t epoch,
                                   bool userProvidedProducerName, bool encrypted) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::PRODUCER);
    CommandProducer* producer = cmd.mutable_producer();
    producer->set_topic(topic);
    producer->set_producer_id(producerId);
    producer->set_request_id(requestId);
    producer->set_epoch(epoch);
    producer->set_user_provided_producer_name(userProvidedProducerName);
    producer->set_encrypted(encrypted);

    for (std::map<std::string, std::string>::const_iterator it = metadata.begin(); it != metadata.end();
         it++) {
        proto::KeyValue* keyValue = proto::KeyValue().New();
        keyValue->set_key(it->first);
        keyValue->set_value(it->second);
        producer->mutable_metadata()->AddAllocated(keyValue);
    }

    if (isBuiltInSchema(schemaInfo.getSchemaType())) {
        producer->set_allocated_schema(getSchema(schemaInfo));
    }

    if (!producerName.empty()) {
        producer->set_producer_name(producerName);
    }

    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newAck(uint64_t consumerId, const MessageIdData& messageId, CommandAck_AckType ackType,
                              int validationError) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::ACK);
    CommandAck* ack = cmd.mutable_ack();
    ack->set_consumer_id(consumerId);
    ack->set_ack_type(ackType);
    if (CommandAck_AckType_IsValid(validationError)) {
        ack->set_validation_error((CommandAck_ValidationError)validationError);
    }
    *(ack->add_message_id()) = messageId;
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newMultiMessageAck(uint64_t consumerId, const std::set<MessageId>& msgIds) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::ACK);
    CommandAck* ack = cmd.mutable_ack();
    ack->set_consumer_id(consumerId);
    ack->set_ack_type(CommandAck_AckType_Individual);
    for (const auto& msgId : msgIds) {
        auto newMsgId = ack->add_message_id();
        newMsgId->set_ledgerid(msgId.ledgerId());
        newMsgId->set_entryid(msgId.entryId());
    }
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newFlow(uint64_t consumerId, uint32_t messagePermits) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::FLOW);
    CommandFlow* flow = cmd.mutable_flow();
    flow->set_consumer_id(consumerId);
    flow->set_messagepermits(messagePermits);
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newCloseProducer(uint64_t producerId, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::CLOSE_PRODUCER);
    CommandCloseProducer* close = cmd.mutable_close_producer();
    close->set_producer_id(producerId);
    close->set_request_id(requestId);
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newCloseConsumer(uint64_t consumerId, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::CLOSE_CONSUMER);
    CommandCloseConsumer* close = cmd.mutable_close_consumer();
    close->set_consumer_id(consumerId);
    close->set_request_id(requestId);
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newPing() {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::PING);
    cmd.mutable_ping();
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newPong() {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::PONG);
    cmd.mutable_pong();
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newRedeliverUnacknowledgedMessages(uint64_t consumerId,
                                                          const std::set<MessageId>& messageIds) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES);
    CommandRedeliverUnacknowledgedMessages* command = cmd.mutable_redeliverunacknowledgedmessages();
    command->set_consumer_id(consumerId);
    for (const auto& msgId : messageIds) {
        MessageIdData* msgIdData = command->add_message_ids();
        msgIdData->set_ledgerid(msgId.ledgerId());
        msgIdData->set_entryid(msgId.entryId());
    }
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newSeek(uint64_t consumerId, uint64_t requestId, const MessageId& messageId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::SEEK);
    CommandSeek* commandSeek = cmd.mutable_seek();
    commandSeek->set_consumer_id(consumerId);
    commandSeek->set_request_id(requestId);

    MessageIdData& messageIdData = *commandSeek->mutable_message_id();
    messageIdData.set_ledgerid(messageId.ledgerId());
    messageIdData.set_entryid(messageId.entryId());
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newSeek(uint64_t consumerId, uint64_t requestId, uint64_t timestamp) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::SEEK);
    CommandSeek* commandSeek = cmd.mutable_seek();
    commandSeek->set_consumer_id(consumerId);
    commandSeek->set_request_id(requestId);
    commandSeek->set_message_publish_time(timestamp);
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newGetLastMessageId(uint64_t consumerId, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::GET_LAST_MESSAGE_ID);

    CommandGetLastMessageId* getLastMessageId = cmd.mutable_getlastmessageid();
    getLastMessageId->set_consumer_id(consumerId);
    getLastMessageId->set_request_id(requestId);
    const SharedBuffer buffer = writeMessageWithSize(cmd);
    cmd.clear_getlastmessageid();
    return buffer;
}

SharedBuffer Commands::newGetTopicsOfNamespace(const std::string& nsName, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::GET_TOPICS_OF_NAMESPACE);
    CommandGetTopicsOfNamespace* getTopics = cmd.mutable_gettopicsofnamespace();
    getTopics->set_request_id(requestId);
    getTopics->set_namespace_(nsName);

    const SharedBuffer buffer = writeMessageWithSize(cmd);
    cmd.clear_gettopicsofnamespace();
    return buffer;
}

std::string Commands::messageType(BaseCommand_Type type) {
    switch (type) {
        case BaseCommand::CONNECT:
            return "CONNECT";
            break;
        case BaseCommand::CONNECTED:
            return "CONNECTED";
            break;
        case BaseCommand::SUBSCRIBE:
            return "SUBSCRIBE";
            break;
        case BaseCommand::PRODUCER:
            return "PRODUCER";
            break;
        case BaseCommand::SEND:
            return "SEND";
            break;
        case BaseCommand::SEND_RECEIPT:
            return "SEND_RECEIPT";
            break;
        case BaseCommand::SEND_ERROR:
            return "SEND_ERROR";
            break;
        case BaseCommand::MESSAGE:
            return "MESSAGE";
            break;
        case BaseCommand::ACK:
            return "ACK";
            break;
        case BaseCommand::FLOW:
            return "FLOW";
            break;
        case BaseCommand::UNSUBSCRIBE:
            return "UNSUBSCRIBE";
            break;
        case BaseCommand::SUCCESS:
            return "SUCCESS";
            break;
        case BaseCommand::ERROR:
            return "ERROR";
            break;
        case BaseCommand::CLOSE_PRODUCER:
            return "CLOSE_PRODUCER";
            break;
        case BaseCommand::CLOSE_CONSUMER:
            return "CLOSE_CONSUMER";
            break;
        case BaseCommand::PRODUCER_SUCCESS:
            return "PRODUCER_SUCCESS";
            break;
        case BaseCommand::PING:
            return "PING";
            break;
        case BaseCommand::PONG:
            return "PONG";
            break;
        case BaseCommand::PARTITIONED_METADATA:
            return "PARTITIONED_METADATA";
            break;
        case BaseCommand::PARTITIONED_METADATA_RESPONSE:
            return "PARTITIONED_METADATA_RESPONSE";
            break;
        case BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES:
            return "REDELIVER_UNACKNOWLEDGED_MESSAGES";
            break;
        case BaseCommand::LOOKUP:
            return "LOOKUP";
            break;
        case BaseCommand::LOOKUP_RESPONSE:
            return "LOOKUP_RESPONSE";
            break;
        case BaseCommand::CONSUMER_STATS:
            return "CONSUMER_STATS";
            break;
        case BaseCommand::CONSUMER_STATS_RESPONSE:
            return "CONSUMER_STATS_RESPONSE";
            break;
        case BaseCommand::REACHED_END_OF_TOPIC:
            return "REACHED_END_OF_TOPIC";
            break;
        case BaseCommand::SEEK:
            return "SEEK";
            break;
        case BaseCommand::ACTIVE_CONSUMER_CHANGE:
            return "ACTIVE_CONSUMER_CHANGE";
            break;
        case BaseCommand::GET_LAST_MESSAGE_ID:
            return "GET_LAST_MESSAGE_ID";
            break;
        case BaseCommand::GET_LAST_MESSAGE_ID_RESPONSE:
            return "GET_LAST_MESSAGE_ID_RESPONSE";
            break;
        case BaseCommand::GET_TOPICS_OF_NAMESPACE:
            return "GET_TOPICS_OF_NAMESPACE";
            break;
        case BaseCommand::GET_TOPICS_OF_NAMESPACE_RESPONSE:
            return "GET_TOPICS_OF_NAMESPACE_RESPONSE";
            break;
        case BaseCommand::GET_SCHEMA:
            return "GET_SCHEMA";
            break;
        case BaseCommand::GET_SCHEMA_RESPONSE:
            return "GET_SCHEMA_RESPONSE";
            break;
        case BaseCommand::AUTH_CHALLENGE:
            return "AUTH_CHALLENGE";
            break;
        case BaseCommand::AUTH_RESPONSE:
            return "AUTH_RESPONSE";
            break;
        case BaseCommand::ACK_RESPONSE:
            return "ACK_RESPONSE";
            break;
        case BaseCommand::GET_OR_CREATE_SCHEMA:
            return "GET_OR_CREATE_SCHEMA";
        case BaseCommand::GET_OR_CREATE_SCHEMA_RESPONSE:
            return "GET_OR_CREATE_SCHEMA_RESPONSE";
        case BaseCommand::NEW_TXN:
            return "NEW_TXN";
            break;
        case BaseCommand::NEW_TXN_RESPONSE:
            return "NEW_TXN_RESPONSE";
            break;
        case BaseCommand::ADD_PARTITION_TO_TXN:
            return "ADD_PARTITION_TO_TXN";
            break;
        case BaseCommand::ADD_PARTITION_TO_TXN_RESPONSE:
            return "ADD_PARTITION_TO_TXN_RESPONSE";
            break;
        case BaseCommand::ADD_SUBSCRIPTION_TO_TXN:
            return "ADD_SUBSCRIPTION_TO_TXN";
            break;
        case BaseCommand::ADD_SUBSCRIPTION_TO_TXN_RESPONSE:
            return "ADD_SUBSCRIPTION_TO_TXN_RESPONSE";
            break;
        case BaseCommand::END_TXN:
            return "END_TXN";
            break;
        case BaseCommand::END_TXN_RESPONSE:
            return "END_TXN_RESPONSE";
            break;
        case BaseCommand::END_TXN_ON_PARTITION:
            return "END_TXN_ON_PARTITION";
            break;
        case BaseCommand::END_TXN_ON_PARTITION_RESPONSE:
            return "END_TXN_ON_PARTITION_RESPONSE";
            break;
        case BaseCommand::END_TXN_ON_SUBSCRIPTION:
            return "END_TXN_ON_SUBSCRIPTION";
            break;
        case BaseCommand::END_TXN_ON_SUBSCRIPTION_RESPONSE:
            return "END_TXN_ON_SUBSCRIPTION_RESPONSE";
            break;
        case BaseCommand::TC_CLIENT_CONNECT_REQUEST:
            return "TC_CLIENT_CONNECT_REQUEST";
        case BaseCommand::TC_CLIENT_CONNECT_RESPONSE:
            return "TC_CLIENT_CONNECT_RESPONSE";
            break;
    };
    BOOST_THROW_EXCEPTION(std::logic_error("Invalid BaseCommand enumeration value"));
}

void Commands::initBatchMessageMetadata(const Message& msg, pulsar::proto::MessageMetadata& batchMetadata) {
    // metadata has already been set in ProducerImpl::setMessageMetadata
    const proto::MessageMetadata& metadata = msg.impl_->metadata;

    // required fields
    batchMetadata.set_producer_name(metadata.producer_name());
    batchMetadata.set_sequence_id(metadata.sequence_id());
    batchMetadata.set_publish_time(metadata.publish_time());

    // optional fields
    if (metadata.has_partition_key()) {
        batchMetadata.set_partition_key(metadata.partition_key());
    }
    if (metadata.has_ordering_key()) {
        batchMetadata.set_ordering_key(metadata.ordering_key());
    }
    if (metadata.has_replicated_from()) {
        batchMetadata.set_replicated_from(metadata.replicated_from());
    }
    if (metadata.replicate_to_size() > 0) {
        for (int i = 0; i < metadata.replicate_to_size(); i++) {
            batchMetadata.add_replicate_to(metadata.replicate_to(i));
        }
    }
    // TODO: set other optional fields
}

uint64_t Commands::serializeSingleMessageInBatchWithPayload(const Message& msg, SharedBuffer& batchPayLoad,
                                                            unsigned long maxMessageSizeInBytes) {
    SingleMessageMetadata metadata;
    if (msg.impl_->hasPartitionKey()) {
        metadata.set_partition_key(msg.impl_->getPartitionKey());
    }

    for (MessageBuilder::StringMap::const_iterator it = msg.impl_->properties().begin();
         it != msg.impl_->properties().end(); it++) {
        proto::KeyValue* keyValue = proto::KeyValue().New();
        keyValue->set_key(it->first);
        keyValue->set_value(it->second);
        metadata.mutable_properties()->AddAllocated(keyValue);
    }

    if (msg.impl_->getEventTimestamp() != 0) {
        metadata.set_event_time(msg.impl_->getEventTimestamp());
    }

    // Format of batch message
    // Each Message = [METADATA_SIZE][METADATA] [PAYLOAD]

    int payloadSize = msg.impl_->payload.readableBytes();
    metadata.set_payload_size(payloadSize);

    int msgMetadataSize = metadata.ByteSize();

    unsigned long requiredSpace = sizeof(uint32_t) + msgMetadataSize + payloadSize;
    if (batchPayLoad.writableBytes() <= sizeof(uint32_t) + msgMetadataSize + payloadSize) {
        LOG_DEBUG("remaining size of batchPayLoad buffer ["
                  << batchPayLoad.writableBytes() << "] can't accomodate new payload [" << requiredSpace
                  << "] - expanding the batchPayload buffer");
        uint32_t new_size =
            std::min(batchPayLoad.readableBytes() * 2, static_cast<uint32_t>(maxMessageSizeInBytes));
        new_size = std::max(new_size, batchPayLoad.readableBytes() + static_cast<uint32_t>(requiredSpace));
        SharedBuffer buffer = SharedBuffer::allocate(new_size);
        // Adding batch created so far
        buffer.write(batchPayLoad.data(), batchPayLoad.readableBytes());
        batchPayLoad = buffer;
    }
    // Adding the new message
    batchPayLoad.writeUnsignedInt(msgMetadataSize);
    metadata.SerializeToArray(batchPayLoad.mutableData(), msgMetadataSize);
    batchPayLoad.bytesWritten(msgMetadataSize);
    batchPayLoad.write(msg.impl_->payload.data(), payloadSize);

    return msg.impl_->metadata.sequence_id();
}

Message Commands::deSerializeSingleMessageInBatch(Message& batchedMessage, int32_t batchIndex) {
    SharedBuffer& uncompressedPayload = batchedMessage.impl_->payload;

    // Format of batch message
    // Each Message = [METADATA_SIZE][METADATA] [PAYLOAD]

    const int& singleMetaSize = uncompressedPayload.readUnsignedInt();
    SingleMessageMetadata metadata;
    metadata.ParseFromArray(uncompressedPayload.data(), singleMetaSize);
    uncompressedPayload.consume(singleMetaSize);

    const int& payloadSize = metadata.payload_size();

    // Get a slice of size payloadSize from offset readIndex_
    SharedBuffer payload = uncompressedPayload.slice(0, payloadSize);
    uncompressedPayload.consume(payloadSize);

    const MessageId& m = batchedMessage.impl_->messageId;
    MessageId singleMessageId(m.partition(), m.ledgerId(), m.entryId(), batchIndex);
    Message singleMessage(singleMessageId, batchedMessage.impl_->metadata, payload, metadata,
                          batchedMessage.impl_->getTopicName());
    singleMessage.impl_->cnx_ = batchedMessage.impl_->cnx_;

    return singleMessage;
}

bool Commands::peerSupportsGetLastMessageId(int32_t peerVersion) { return peerVersion >= proto::v12; }

bool Commands::peerSupportsActiveConsumerListener(int32_t peerVersion) { return peerVersion >= proto::v12; }

bool Commands::peerSupportsMultiMessageAcknowledgement(int32_t peerVersion) {
    return peerVersion >= proto::v12;
}

bool Commands::peerSupportsJsonSchemaAvroFormat(int32_t peerVersion) { return peerVersion >= proto::v13; }

bool Commands::peerSupportsGetOrCreateSchema(int32_t peerVersion) { return peerVersion >= proto::v15; }
}  // namespace pulsar
/* namespace pulsar */
