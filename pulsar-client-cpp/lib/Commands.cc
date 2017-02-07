/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Commands.h"
#include "MessageImpl.h"
#include "Version.h"
#include "pulsar/MessageBuilder.h"
#include "LogUtils.h"
#include "checksum/ChecksumProvider.h"
#include <algorithm>
namespace pulsar {

using namespace pulsar::proto;

DECLARE_LOG_OBJECT();

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

SharedBuffer Commands::newPartitionMetadataRequest(BaseCommand& cmd, const std::string& topic, uint64_t requestId) {
    cmd.set_type(BaseCommand::PARTITIONED_METADATA);
    CommandPartitionedTopicMetadata* partitionMetadata = cmd.mutable_partitionmetadata();
    partitionMetadata->set_topic(topic);
    partitionMetadata->set_request_id(requestId);
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newLookup(BaseCommand& cmd, const std::string& topic, const bool authoritative,
                                 uint64_t requestId) {
    cmd.set_type(BaseCommand::LOOKUP);
    CommandLookupTopic* lookup = cmd.mutable_lookuptopic();
    lookup->set_topic(topic);
    lookup->set_authoritative(authoritative);
    lookup->set_request_id(requestId);
    return writeMessageWithSize(cmd);
}

PairSharedBuffer Commands::newSend(SharedBuffer& headers, BaseCommand& cmd,
                                   uint64_t producerId, uint64_t sequenceId, ChecksumType checksumType, const Message& msg) {
    const proto::MessageMetadata& metadata = msg.impl_->metadata;
    SharedBuffer& payload = msg.impl_->payload;

    cmd.set_type(BaseCommand::SEND);
    CommandSend* send = cmd.mutable_send();
    send->set_producer_id(producerId);
    send->set_sequence_id(sequenceId);

    // / Wire format
    // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]

    int cmdSize = cmd.ByteSize();
    int msgMetadataSize = metadata.ByteSize();
    int payloadSize = payload.readableBytes();

    int magicAndChecksumLength =
            (Crc32c == (checksumType)) ? (2 + 4 /* magic + checksumLength*/) : 0;
    bool includeChecksum = magicAndChecksumLength > 0;
    int headerContentSize = 4 + cmdSize + magicAndChecksumLength + 4 + msgMetadataSize;  // cmdLength + cmdSize + magicLength +
    // checksumSize + msgMetadataLength + msgMetadataSize
    int totalSize = headerContentSize + payloadSize;
    int headersSize = 4 + headerContentSize;  // totalSize + headerLength
    int checksumReaderIndex = -1;

    headers.reset();
    assert(headers.writableBytes() >= headersSize);
    headers.writeUnsignedInt(totalSize);  // External frame

    // Write cmd
    headers.writeUnsignedInt(cmdSize);
    cmd.SerializeToArray(headers.mutableData(), cmdSize);
    headers.bytesWritten(cmdSize);

    // Create checksum placeholder
    if (includeChecksum) {
        headers.writeUnsignedShort(magicCrc32c);
        checksumReaderIndex = headers.writerIndex();
        headers.skipBytes(checksumSize);  //skip 4 bytes of checksum
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
        uint32_t metadataChecksum = computeChecksum(
                0, headers.data() + metadataStartIndex, (writeIndex - metadataStartIndex));
        uint32_t computedChecksum = computeChecksum(metadataChecksum, payload.data(),
                                               payload.writerIndex());
        // set computed checksum
        headers.setWriterIndex(checksumReaderIndex);
        headers.writeUnsignedInt(computedChecksum);
        headers.setWriterIndex(writeIndex);
    }

    return composite;
}

SharedBuffer Commands::newConnect(const AuthenticationPtr& authentication) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::CONNECT);
    CommandConnect* connect = cmd.mutable_connect();
    connect->set_client_version(_PULSAR_VERSION_);
    connect->set_auth_method_name(authentication->getAuthMethodName());
    connect->set_protocol_version(ProtocolVersion_MAX);
    std::string authDataContent;
    if (authentication->getAuthData(authDataContent) == ResultOk) {
        connect->set_auth_data(authDataContent);
    }
    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newSubscribe(const std::string& topic, const std::string&subscription,
                                    uint64_t consumerId, uint64_t requestId,
                                    CommandSubscribe_SubType subType, const std::string& consumerName) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::SUBSCRIBE);
    CommandSubscribe* subscribe = cmd.mutable_subscribe();
    subscribe->set_topic(topic);
    subscribe->set_subscription(subscription);
    subscribe->set_subtype(subType);
    subscribe->set_consumer_id(consumerId);
    subscribe->set_request_id(requestId);
    subscribe->set_consumer_name(consumerName);

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
                                   const std::string& producerName, uint64_t requestId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::PRODUCER);
    CommandProducer* producer = cmd.mutable_producer();
    producer->set_topic(topic);
    producer->set_producer_id(producerId);
    producer->set_request_id(requestId);

    if (!producerName.empty()) {
        producer->set_producer_name(producerName);
    }

    return writeMessageWithSize(cmd);
}

SharedBuffer Commands::newAck(uint64_t consumerId, const MessageIdData& messageId,
                              CommandAck_AckType ackType,
                              int validationError) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::ACK);
    CommandAck* ack = cmd.mutable_ack();
    ack->set_consumer_id(consumerId);
    ack->set_ack_type(ackType);
    if (CommandAck_AckType_IsValid(validationError)) {
        ack->set_validation_error((CommandAck_ValidationError) validationError);
    }
    *(ack->mutable_message_id()) = messageId;
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

SharedBuffer Commands::newRedeliverUnacknowledgedMessages(uint64_t consumerId) {
    BaseCommand cmd;
    cmd.set_type(BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES);
    CommandRedeliverUnacknowledgedMessages* command = cmd.mutable_redeliverunacknowledgedmessages();
    command->set_consumer_id(consumerId);
    return writeMessageWithSize(cmd);
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
    };
}

void Commands::initBatchMessageMetadata(const Message &msg, pulsar::proto::MessageMetadata &batchMetadata)
{
    if (msg.impl_->metadata.has_publish_time()) {
        batchMetadata.set_publish_time(msg.impl_->metadata.publish_time());
    }

    if (msg.impl_->metadata.has_sequence_id()) {
        batchMetadata.set_sequence_id(msg.impl_->metadata.sequence_id());
    }

    if (msg.impl_->metadata.has_replicated_from()) {
        batchMetadata.set_replicated_from(msg.impl_->metadata.replicated_from());
    }
}

void Commands::serializeSingleMessageInBatchWithPayload(const Message &msg,
                                                        SharedBuffer& batchPayLoad, const unsigned long& maxMessageSizeInBytes) {
    SingleMessageMetadata metadata;
    if (msg.impl_->hasPartitionKey()) {
        metadata.set_partition_key(msg.impl_->getPartitionKey());
    }

    for (MessageBuilder::StringMap::const_iterator it = msg.impl_->properties().begin();
            it != msg.impl_->properties().end(); it++) {
        proto::KeyValue *keyValue = proto::KeyValue().New();
        keyValue->set_key(it->first);
        keyValue->set_value(it->second);
        metadata.mutable_properties()->AddAllocated(keyValue);
    }

    // Format of batch message
    // Each Message = [METADATA_SIZE][METADATA] [PAYLOAD]

    int payloadSize = msg.impl_->payload.readableBytes();
    metadata.set_payload_size(payloadSize);

    int msgMetadataSize = metadata.ByteSize();

    unsigned long requiredSpace = sizeof(uint32_t) + msgMetadataSize + payloadSize;
    if (batchPayLoad.writableBytes() <= sizeof(uint32_t) + msgMetadataSize + payloadSize) {
        LOG_DEBUG("remaining size of batchPayLoad buffer [" << batchPayLoad.writableBytes() << "] can't accomodate new payload [" << requiredSpace << "] - expanding the batchPayload buffer");
        SharedBuffer buffer = SharedBuffer::allocate(batchPayLoad.readableBytes() +  std::max(requiredSpace, maxMessageSizeInBytes));
        // Adding batch created so far
        buffer.write(batchPayLoad.data(), batchPayLoad.readableBytes());
        batchPayLoad = buffer;
    }
    // Adding the new message
    batchPayLoad.writeUnsignedInt(msgMetadataSize);
    metadata.SerializeToArray(batchPayLoad.mutableData(), msgMetadataSize);
    batchPayLoad.bytesWritten(msgMetadataSize);
    batchPayLoad.write(msg.impl_->payload.data(), payloadSize);
}

Message Commands::deSerializeSingleMessageInBatch(Message& batchedMessage) {
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

    Message singleMessage(batchedMessage.impl_->messageId, batchedMessage.impl_->metadata, payload,
                          metadata);
    singleMessage.impl_->cnx_ = batchedMessage.impl_->cnx_;

    return singleMessage;
}
}
/* namespace pulsar */
