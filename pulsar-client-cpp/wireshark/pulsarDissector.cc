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
#include <glib.h>
#include <config.h>
#include <epan/expert.h>
#include <epan/packet.h>
#include <epan/prefs.h>
#include <epan/proto.h>
#include <epan/column-utils.h>
#include <epan/dissectors/packet-tcp.h>
#include <epan/value_string.h>
#include <wsutil/nstime.h>

#include "PulsarApi.pb.h"

#ifdef VERSION
#undef VERSION
#endif

/* Version number of package */
#define VERSION "0.0.1"

const static int PULSAR_PORT = 6650;

static int proto_pulsar = -1;
static int hf_pulsar_error = -1;
static int hf_pulsar_error_message = -1;
static int hf_pulsar_cmd_type = -1;
static int hf_pulsar_frame_size = -1;
static int hf_pulsar_cmd_size = -1;

static int hf_pulsar_client_version = -1;
static int hf_pulsar_auth_method = -1;
static int hf_pulsar_auth_data = -1;
static int hf_pulsar_protocol_version = -1;
static int hf_pulsar_server_version = -1;

static int hf_pulsar_topic = -1;
static int hf_pulsar_subscription = -1;
static int hf_pulsar_subType = -1;
static int hf_pulsar_consumer_id = -1;
static int hf_pulsar_producer_id = -1;
static int hf_pulsar_server_error = -1;
static int hf_pulsar_ack_type = -1;
static int hf_pulsar_request_id = -1;
static int hf_pulsar_consumer_name = -1;
static int hf_pulsar_producer_name = -1;

static int hf_pulsar_publish_time = -1;
static int hf_pulsar_deliver_at_time = -1;
static int hf_pulsar_event_time = -1;
static int hf_pulsar_deliver_after_time = -1;

static int hf_pulsar_chunk_id = -1;
static int hf_pulsar_num_chunks_from_msg = -1;
static int hf_pulsar_uuid = -1;
static int hf_pulsar_compression_type = -1;
static int hf_pulsar_uncompressed_size = -1;
static int hf_pulsar_partition_key = -1;
static int hf_pulsar_ordering_key = -1;
static int hf_pulsar_encryption_algo = -1;
static int hf_pulsar_encryption_param = -1;
static int hf_pulsar_encryption_keys = -1;
static int hf_pulsar_replicated_from = -1;
static int hf_pulsar_replicate_to = -1;
static int hf_pulsar_property = -1;

static int hf_pulsar_txnid_least_bits = -1;
static int hf_pulsar_txnid_most_bits = -1;

static int hf_pulsar_request_in = -1;
static int hf_pulsar_response_in = -1;
static int hf_pulsar_publish_latency = -1;

static int hf_pulsar_sequence_id = -1;
static int hf_pulsar_highest_sequence_id = -1;
static int hf_pulsar_message_id = -1;
static int hf_pulsar_message_permits = -1;

static int ett_pulsar = -1;

const static int FRAME_SIZE_LEN = 4;
const static guint16 MAGIC_BROKER_ENTRY_METADATA = 0x0e02;
const static guint16 MAGIC_CRC32C = 0x0e01;

static pulsar::proto::BaseCommand command;

using namespace pulsar::proto;

static const value_string pulsar_cmd_names[] = {
    {BaseCommand::CONNECT, "Connect"},
    {BaseCommand::CONNECTED, "Connected"},
    {BaseCommand::SUBSCRIBE, "Subscribe"},
    {BaseCommand::PRODUCER, "Producer"},
    {BaseCommand::SEND, "Send"},
    {BaseCommand::SEND_RECEIPT, "SendReceipt"},
    {BaseCommand::SEND_ERROR, "SendError"},
    {BaseCommand::MESSAGE, "Message"},
    {BaseCommand::ACK, "Ack"},
    {BaseCommand::FLOW, "Flow"},
    {BaseCommand::UNSUBSCRIBE, "Unsubscribe"},
    {BaseCommand::SUCCESS, "Success"},
    {BaseCommand::ERROR, "Error"},
    {BaseCommand::CLOSE_PRODUCER, "CloseProducer"},
    {BaseCommand::CLOSE_CONSUMER, "CloseConsumer"},
    {BaseCommand::PRODUCER_SUCCESS, "ProducerSuccess"},
    {BaseCommand::PING, "Ping"},
    {BaseCommand::PONG, "Pong"},
    {BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES, "RedeliverUnacknowledgedMessages"},
    {BaseCommand::PARTITIONED_METADATA, "PartitionedMetadata"},
    {BaseCommand::PARTITIONED_METADATA_RESPONSE, "PartitionedMetadataResponse"},
    {BaseCommand::LOOKUP, "Lookup"},
    {BaseCommand::LOOKUP_RESPONSE, "LookupResponse"},
    {BaseCommand::CONSUMER_STATS, "ConsumerStats"},
    {BaseCommand::CONSUMER_STATS_RESPONSE, "ConsumerStatsResponse"},
    {BaseCommand::SEEK, "Seek"},
    {BaseCommand::GET_LAST_MESSAGE_ID, "GetLastMessageId"},
    {BaseCommand::GET_LAST_MESSAGE_ID_RESPONSE, "GetLastMessageIdResponse"},
    {BaseCommand::ACTIVE_CONSUMER_CHANGE, "ActiveConsumerChange"},
    {BaseCommand::GET_TOPICS_OF_NAMESPACE, "GetTopicsOfNamespace"},
    {BaseCommand::GET_TOPICS_OF_NAMESPACE_RESPONSE, "GetTopicsOfNamespaceResponse"},
    {BaseCommand::GET_SCHEMA, "GetSchema"},
    {BaseCommand::GET_SCHEMA_RESPONSE, "GetSchemaResponse"},
    {BaseCommand::AUTH_CHALLENGE, "AuthChallenge"},
    {BaseCommand::AUTH_RESPONSE, "AuthResponse"},
    {BaseCommand::ACK_RESPONSE, "AckResponse"},
    {BaseCommand::GET_OR_CREATE_SCHEMA, "GetOrCreateSchema"},
    {BaseCommand::AUTH_CHALLENGE, "AuthChallenge"},
    {BaseCommand::AUTH_RESPONSE, "AuthResponse"},
    {BaseCommand::ACK_RESPONSE, "AckResponse"},
    {BaseCommand::GET_OR_CREATE_SCHEMA, "GetOrCreateSchema"},
    {BaseCommand::GET_OR_CREATE_SCHEMA_RESPONSE, "GetOrCreateSchemaResponse"},
    {BaseCommand::NEW_TXN, "NewTxn"},
    {BaseCommand::NEW_TXN_RESPONSE, "NewTxnResponse"},
    {BaseCommand::ADD_PARTITION_TO_TXN, "AddPartitionToTxn"},
    {BaseCommand::ADD_SUBSCRIPTION_TO_TXN, "AddSubscriptionToTxn"},
    {BaseCommand::ADD_SUBSCRIPTION_TO_TXN_RESPONSE, "AddSubscriptionToTxnResponse"},
    {BaseCommand::END_TXN, "EndTxn"},
    {BaseCommand::END_TXN_RESPONSE, "EndTxnResponse"},
    {BaseCommand::END_TXN_ON_PARTITION, "EndTxnOnPartition"},
    {BaseCommand::END_TXN_ON_PARTITION_RESPONSE, "EndTxnOnPartitionResponse"},
    {BaseCommand::END_TXN_ON_SUBSCRIPTION, "EndTxnOnSubscription"},
    {BaseCommand::END_TXN_ON_SUBSCRIPTION_RESPONSE, "EndTxnOnSubscriptionResponse"},
    {BaseCommand::TC_CLIENT_CONNECT_REQUEST, "TcClientConnectRequest"},
    {BaseCommand::TC_CLIENT_CONNECT_RESPONSE, "TcClientConnectResponse"},
};

static const value_string auth_methods_vs[] = {
    {AuthMethodNone, "None"},     //
    {AuthMethodYcaV1, "YCAv1"},   //
    {AuthMethodAthens, "Athens"}  //
};

static const value_string server_errors_vs[] = {
    {UnknownError, "UnknownError"},
    {MetadataError, "MetadataError"},
    {PersistenceError, "PersistenceError"},
    {AuthenticationError, "AuthenticationError"},
    {AuthorizationError, "AuthorizationError"},
    {ConsumerBusy, "ConsumerBusy"},
    {ServiceNotReady, "ServiceNotReady"},
    {ProducerBlockedQuotaExceededError, "ProducerBlockedQuotaExceededError"},
    {ProducerBlockedQuotaExceededException, "ProducerBlockedQuotaExceededException"},
    {ChecksumError, "ChecksumError"},
    {UnsupportedVersionError, "UnsupportedVersionError"},
    {TopicNotFound, "TopicNotFound"},
    {SubscriptionNotFound, "SubscriptionNotFound"},
    {ConsumerNotFound, "ConsumerNotFound"},
    {TooManyRequests, "TooManyRequests"},
    {TopicTerminatedError, "TopicTerminatedError"},
    {ProducerBusy, "ProducerBusy"},
    {InvalidTopicName, "InvalidTopicName"},
    {IncompatibleSchema, "IncompatibleSchema"},
    {ConsumerAssignError, "ConsumerAssignError"},
    {TransactionCoordinatorNotFound, "TransactionCoordinatorNotFound"},
    {InvalidTxnStatus, "InvalidTxnStatus"},
    {NotAllowedError, "NotAllowedError"},
    {TransactionConflict, "TransactionConflict"},
    {TransactionNotFound, "TransactionNotFound"},
    {ProducerFenced, "ProducerFenced"},
};

static const value_string ack_type_vs[] = {{CommandAck::Individual, "Individual"},
                                           {CommandAck::Cumulative, "Cumulative"}};

static const value_string protocol_version_vs[] = {
    {v0, "v0"},   {v1, "v1"},   {v2, "v2"},   {v3, "v3"},   {v4, "v4"},   {v5, "v5"},   {v6, "v6"},
    {v7, "v7"},   {v8, "v8"},   {v9, "v9"},   {v10, "v10"}, {v11, "v11"}, {v12, "v12"}, {v13, "v13"},
    {v14, "v14"}, {v15, "v15"}, {v16, "v16"}, {v17, "v17"}, {v18, "v18"}, {v19, "v19"},
};

static const value_string sub_type_names_vs[] = {
    {CommandSubscribe::Exclusive, "Exclusive"},
    {CommandSubscribe::Shared, "Shared"},
    {CommandSubscribe::Failover, "Failover"},
    {CommandSubscribe::Key_Shared, "Key_Shared"},
};

static const value_string compression_type_name_vs[] = {
    {CompressionType::NONE, "NONE"}, {CompressionType::LZ4, "LZ4"},       {CompressionType::ZLIB, "ZLIB"},
    {CompressionType::ZSTD, "ZSTD"}, {CompressionType::SNAPPY, "SNAPPY"},
};

static const char* to_str(int value, const value_string* values) {
    return val_to_str(value, values, "Unknown (%d)");
}

struct MessageIdComparator {
    bool operator()(const MessageIdData& a, const MessageIdData& b) const {
        if (a.ledgerid() < b.ledgerid()) {
            return true;
        } else if (a.ledgerid() == b.ledgerid()) {
            return a.entryid() < b.entryid();
        } else {
            return false;
        }
    }
};

struct RequestData {
    uint32_t requestFrame;
    nstime_t requestTimestamp;
    uint32_t ackFrame;
    nstime_t ackTimestamp;

    RequestData() : requestFrame(UINT32_MAX), ackFrame(UINT32_MAX) {}
};

struct RequestResponseData : public RequestData {
    uint64_t id;  // producer / consumer id
};

struct ProducerData {
    std::string topic;
    std::string producerName;

    std::map<uint64_t, RequestData> messages;
};

struct ConsumerData {
    std::string topic;
    std::string subscriptionName;
    std::string consumerName;
    CommandSubscribe::SubType subType;

    // Link messages to acks for the consumer
    std::map<MessageIdData, RequestData, MessageIdComparator> messages;
};

struct ConnectionState {
    std::map<uint64_t, ProducerData> producers;
    std::map<uint64_t, ConsumerData> consumers;
    std::map<uint64_t, RequestResponseData> requests;
};

static void dissect_message_metadata(proto_tree* frame_tree, tvbuff_t* tvb, int offset, int maxOffset) {
    if (tvb_get_ntohs(tvb, offset) == MAGIC_BROKER_ENTRY_METADATA) {
        offset += 2;
        auto brokerEntryMetadataSize = (int)tvb_get_ntohl(tvb, offset);
        offset += brokerEntryMetadataSize + 2;
#ifdef DEBUG
        proto_tree_add_debug_text(frame_tree, "[DEBUG] MAGIC_BROKER_ENTRY_METADATA %d",
                                  brokerEntryMetadataSize);
#endif
    }
    if (tvb_get_ntohs(tvb, offset) == MAGIC_CRC32C) {
#ifdef DEBUG
        auto checksum = tvb_get_ntohl(tvb, offset);
        proto_tree_add_debug_text(frame_tree, "[DEBUG] CRC32C %d", checksum);
#endif
        // Skip CRC32C Magic (2) and CRC32C checksum (4)
        offset += 2;
        offset += 4;
    }
    // Decode message metadata
    auto metadataSize = tvb_get_ntohl(tvb, offset);
    offset += 4;
#ifdef DEBUG
    proto_tree_add_debug_text(frame_tree, "[DEBUG] MetadataSize %d, maxOffset %d", metadataSize, maxOffset);
#endif

    if (offset + metadataSize > maxOffset) {
    // Not enough data to dissect metadata
#ifdef DEBUG
        proto_tree_add_debug_text(frame_tree, "[DEBUG] Not enough data to dissect message metadata");
#endif
        return;
    }

    static MessageMetadata msgMetadata;
    auto ptr = tvb_get_ptr(tvb, offset, metadataSize);

    if (!msgMetadata.ParseFromArray(ptr, metadataSize)) {
        proto_tree_add_boolean_format(frame_tree, hf_pulsar_error, tvb, offset, metadataSize, true,
                                      "Error parsing protocol buffer message metadata");
        return;
    }

#ifdef DEBUG
    proto_tree_add_debug_text(frame_tree, "[DEBUG] MessageMetadata Utf8DebugString : %s",
                              msgMetadata.Utf8DebugString().c_str());
    proto_tree_add_debug_text(frame_tree, "[DEBUG] MessageMetadata SerializeAsString : %s",
                              msgMetadata.SerializeAsString().c_str());
#endif

    proto_item* md_tree =
        proto_tree_add_subtree_format(frame_tree, tvb, offset, metadataSize, ett_pulsar, nullptr,
                                      "MessageMetadata / %s / %" G_GINT64_MODIFIER "u",
                                      msgMetadata.producer_name().c_str(), msgMetadata.sequence_id());
    proto_tree_add_string(md_tree, hf_pulsar_producer_name, tvb, offset, metadataSize,
                          msgMetadata.producer_name().c_str());

    // IDs
    proto_tree_add_uint64(md_tree, hf_pulsar_sequence_id, tvb, offset, metadataSize,
                          msgMetadata.sequence_id());
    if (msgMetadata.has_highest_sequence_id()) {
        proto_tree_add_uint64(md_tree, hf_pulsar_highest_sequence_id, tvb, offset, metadataSize,
                              msgMetadata.highest_sequence_id());
    }

    if (msgMetadata.has_chunk_id()) {
        proto_tree_add_uint(md_tree, hf_pulsar_chunk_id, tvb, offset, metadataSize, msgMetadata.chunk_id());
        if (msgMetadata.has_num_chunks_from_msg()) {
            proto_tree_add_uint(md_tree, hf_pulsar_num_chunks_from_msg, tvb, offset, metadataSize,
                                msgMetadata.num_chunks_from_msg());
        }
    }

    if (msgMetadata.has_uuid()) {
        proto_tree_add_string(md_tree, hf_pulsar_uuid, tvb, offset, metadataSize, msgMetadata.uuid().c_str());
    }

    // Times
    proto_tree_add_uint64(md_tree, hf_pulsar_publish_time, tvb, offset, metadataSize,
                          msgMetadata.publish_time());

    if (msgMetadata.has_deliver_at_time()) {
        proto_tree_add_uint64(md_tree, hf_pulsar_deliver_at_time, tvb, offset, metadataSize,
                              msgMetadata.deliver_at_time());
        proto_tree_add_uint64(md_tree, hf_pulsar_deliver_after_time, tvb, offset, metadataSize,
                              msgMetadata.deliver_at_time() - msgMetadata.publish_time());
    }

    if (msgMetadata.has_event_time()) {
        proto_tree_add_uint64(md_tree, hf_pulsar_event_time, tvb, offset, metadataSize,
                              msgMetadata.event_time());
    }

    // Compression
    if (msgMetadata.has_compression()) {
        proto_tree_add_string(md_tree, hf_pulsar_compression_type, tvb, offset, metadataSize,
                              to_str(msgMetadata.compression(), compression_type_name_vs));
    }

    if (msgMetadata.has_uncompressed_size()) {
        proto_tree_add_uint(md_tree, hf_pulsar_uncompressed_size, tvb, offset, metadataSize,
                            msgMetadata.uncompressed_size());
    }

    // Keys
    if (msgMetadata.has_partition_key()) {
        proto_tree_add_string(md_tree, hf_pulsar_partition_key, tvb, offset, metadataSize,
                              msgMetadata.partition_key().c_str());
    }

    if (msgMetadata.has_ordering_key()) {
        proto_tree_add_string(md_tree, hf_pulsar_ordering_key, tvb, offset, metadataSize,
                              msgMetadata.ordering_key().c_str());
    }

    // Encryption
    if (msgMetadata.has_encryption_algo()) {
        proto_tree_add_string(md_tree, hf_pulsar_encryption_algo, tvb, offset, metadataSize,
                              msgMetadata.encryption_algo().c_str());
    }
    if (msgMetadata.has_encryption_param()) {
        proto_tree_add_string(md_tree, hf_pulsar_encryption_param, tvb, offset, metadataSize,
                              msgMetadata.encryption_param().c_str());
    }
    if (msgMetadata.encryption_keys_size() > 0) {
        proto_item* encryption_keys_tree = proto_tree_add_subtree_format(
            md_tree, tvb, offset, msgMetadata.encryption_param().size(), ett_pulsar, nullptr,
            "EncryptionParam / %s", msgMetadata.encryption_algo().c_str());
        for (int i = 0; i < msgMetadata.encryption_keys().size(); i++) {
            const auto& encryption_key = msgMetadata.encryption_keys(i);
            proto_tree_add_string_format(
                encryption_keys_tree, hf_pulsar_encryption_keys, tvb, offset, metadataSize, "", "%s : %s",
                encryption_key.has_key() ? encryption_key.key().c_str() : "<none>",
                encryption_key.has_value() ? encryption_key.value().c_str() : "<none>");
        }
    }

    // Properties
    if (msgMetadata.properties_size() > 0) {
        proto_item* properties_tree = proto_tree_add_subtree_format(md_tree, tvb, offset, metadataSize,
                                                                    ett_pulsar, nullptr, "Properties");
        for (int i = 0; i < msgMetadata.properties_size(); i++) {
            const KeyValue& kv = msgMetadata.properties(i);
            proto_tree_add_string_format(properties_tree, hf_pulsar_property, tvb, offset, metadataSize, "",
                                         "%s : %s", kv.key().c_str(), kv.value().c_str());
        }
    }

    // Replication
    if (msgMetadata.has_replicated_from()) {
        proto_tree_add_string(md_tree, hf_pulsar_replicated_from, tvb, offset, metadataSize,
                              msgMetadata.replicated_from().c_str());
    }

    if (msgMetadata.replicate_to_size() > 0) {
        proto_item* replicate_tree = proto_tree_add_subtree_format(md_tree, tvb, offset, metadataSize,
                                                                   ett_pulsar, nullptr, "Replicate to");
        for (int i = 0; i < msgMetadata.replicate_to_size(); i++) {
            proto_tree_add_string_format(replicate_tree, hf_pulsar_replicated_from, tvb, offset, metadataSize,
                                         "", "%s", msgMetadata.replicate_to(i).c_str());
        }
    }

    // Transaction
    if (msgMetadata.has_txnid_least_bits()) {
        proto_tree_add_uint64(md_tree, hf_pulsar_txnid_least_bits, tvb, offset, metadataSize,
                              msgMetadata.txnid_least_bits());
    }

    if (msgMetadata.has_txnid_most_bits()) {
        proto_tree_add_uint64(md_tree, hf_pulsar_txnid_most_bits, tvb, offset, metadataSize,
                              msgMetadata.txnid_most_bits());
    }

    // Payloads
    offset += metadataSize;
    uint32_t payloadSize = maxOffset - offset;
    proto_tree_add_subtree_format(md_tree, tvb, offset, payloadSize, ett_pulsar, nullptr, "Payload / size=%u",
                                  payloadSize);
}

void link_to_request_frame(proto_tree* cmd_tree, tvbuff_t* tvb, int offset, int size,
                           const RequestData& reqData) {
    if (reqData.requestFrame != UINT32_MAX) {
        proto_tree* item =
            proto_tree_add_uint(cmd_tree, hf_pulsar_request_in, tvb, offset, size, reqData.requestFrame);
        PROTO_ITEM_SET_GENERATED(item);

        nstime_t latency;
        nstime_delta(&latency, &reqData.ackTimestamp, &reqData.requestTimestamp);
        item = proto_tree_add_time(cmd_tree, hf_pulsar_publish_latency, tvb, offset, size, &latency);
        PROTO_ITEM_SET_GENERATED(item);
    }
}

void link_to_response_frame(proto_tree* cmd_tree, tvbuff_t* tvb, int offset, int size,
                            const RequestData& reqData) {
    if (reqData.ackFrame != UINT32_MAX) {
        proto_tree* item =
            proto_tree_add_uint(cmd_tree, hf_pulsar_response_in, tvb, offset, size, reqData.ackFrame);
        PROTO_ITEM_SET_GENERATED(item);

        nstime_t latency;
        nstime_delta(&latency, &reqData.ackTimestamp, &reqData.requestTimestamp);
        item = proto_tree_add_time(cmd_tree, hf_pulsar_publish_latency, tvb, offset, size, &latency);
        PROTO_ITEM_SET_GENERATED(item);
    }
}

//////////

/* This method dissects fully reassembled messages */
static int dissect_pulsar_message(tvbuff_t* tvb, packet_info* pinfo, proto_tree* tree, void* data _U_) {
    uint32_t offset = FRAME_SIZE_LEN;
    int maxOffset = tvb_captured_length(tvb);
    auto cmdSize = (uint32_t)tvb_get_ntohl(tvb, offset);
    offset += 4;

    if (offset + cmdSize > maxOffset) {
    // Not enough data to dissect
#ifdef DEBUG
        proto_tree_add_debug_text(tree, "[Debug] Not enough data to dissect command");
#endif
        return maxOffset;
    }

    col_set_str(pinfo->cinfo, COL_PROTOCOL, "Pulsar");

    conversation_t* conversation = find_or_create_conversation(pinfo);
    auto state = (ConnectionState*)conversation_get_proto_data(conversation, proto_pulsar);
    if (state == nullptr) {
        state = new ConnectionState();
        conversation_add_proto_data(conversation, proto_pulsar, state);
    }

    auto ptr = (uint8_t*)tvb_get_ptr(tvb, offset, cmdSize);
    if (!command.ParseFromArray(ptr, cmdSize)) {
        proto_tree_add_boolean_format(tree, hf_pulsar_error, tvb, offset, cmdSize, true,
                                      "Error parsing protocol buffer command");
        return maxOffset;
    }

    int cmdOffset = offset;
    offset += cmdSize;

    col_add_str(pinfo->cinfo, COL_INFO, to_str(command.type(), pulsar_cmd_names));

    proto_item* frame_tree = nullptr;
    proto_item* cmd_tree = nullptr;
    if (tree) { /* we are being asked for details */
        proto_item* ti = proto_tree_add_item(tree, proto_pulsar, tvb, 0, -1, ENC_NA);
        frame_tree = proto_item_add_subtree(ti, ett_pulsar);
        proto_tree_add_item(frame_tree, hf_pulsar_frame_size, tvb, 0, 4, ENC_BIG_ENDIAN);
        proto_tree_add_item(frame_tree, hf_pulsar_cmd_size, tvb, 4, 4, ENC_BIG_ENDIAN);
        cmd_tree = proto_tree_add_subtree_format(frame_tree, tvb, 8, cmdSize, ett_pulsar, nullptr,
                                                 "Command %s", to_str(command.type(), pulsar_cmd_names));
        proto_tree_add_string(cmd_tree, hf_pulsar_cmd_type, tvb, 8, cmdSize,
                              to_str(command.type(), pulsar_cmd_names));
    }

    switch (command.type()) {
        case BaseCommand::CONNECT: {
            const CommandConnect& connect = command.connect();
            if (tree) {
                proto_tree_add_string(cmd_tree, hf_pulsar_client_version, tvb, cmdOffset, cmdSize,
                                      connect.client_version().c_str());
                proto_tree_add_string(cmd_tree, hf_pulsar_protocol_version, tvb, cmdOffset, cmdSize,
                                      to_str(connect.protocol_version(), protocol_version_vs));
                proto_tree_add_string(cmd_tree, hf_pulsar_auth_method, tvb, cmdOffset, cmdSize,
                                      to_str(connect.auth_method(), auth_methods_vs));
                if (connect.has_auth_data()) {
                    proto_tree_add_string(cmd_tree, hf_pulsar_auth_data, tvb, cmdOffset, cmdSize,
                                          connect.auth_data().c_str());
                }
            }
            break;
        }
        case BaseCommand::CONNECTED: {
            const CommandConnected& connected = command.connected();
            if (tree) {
                proto_tree_add_string(cmd_tree, hf_pulsar_server_version, tvb, cmdOffset, cmdSize,
                                      connected.server_version().c_str());
                proto_tree_add_string(cmd_tree, hf_pulsar_protocol_version, tvb, cmdOffset, cmdSize,
                                      to_str(connected.protocol_version(), protocol_version_vs));
            }
            break;
        }
        case BaseCommand::SUBSCRIBE: {
            const CommandSubscribe& subscribe = command.subscribe();
            RequestData& reqData = state->requests[subscribe.request_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ConsumerData& consumerData = state->consumers[subscribe.consumer_id()];
            consumerData.topic = subscribe.topic();
            consumerData.subscriptionName = subscribe.subscription();
            consumerData.consumerName = subscribe.consumer_name();
            consumerData.subType = subscribe.subtype();

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %s / %s / %s",
                            to_str(subscribe.subtype(), sub_type_names_vs), subscribe.topic().c_str(),
                            subscribe.subscription().c_str(), subscribe.consumer_name().c_str());

            if (tree) {
                proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                      subscribe.topic().c_str());
                proto_tree_add_string(cmd_tree, hf_pulsar_subscription, tvb, cmdOffset, cmdSize,
                                      subscribe.subscription().c_str());
                proto_tree_add_string(cmd_tree, hf_pulsar_subType, tvb, cmdOffset, cmdSize,
                                      to_str(subscribe.subtype(), sub_type_names_vs));
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      subscribe.consumer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      subscribe.request_id());
                proto_tree_add_string(
                    cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                    subscribe.has_consumer_name() ? subscribe.consumer_name().c_str() : "<none>");
            }
            break;
        }
        case BaseCommand::PRODUCER: {
            const CommandProducer& producer = command.producer();
            RequestResponseData& reqData = state->requests[producer.request_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;
            reqData.id = producer.producer_id();

            state->producers[producer.producer_id()].topic = producer.topic();

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s", producer.topic().c_str());

            if (tree) {
                proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                      producer.topic().c_str());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                      producer.producer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      producer.request_id());
                proto_tree_add_string(
                    cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                    producer.has_producer_name() ? producer.producer_name().c_str() : "<none>");

                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::SEND: {
            const CommandSend& send = command.send();
            RequestData& reqData = state->producers[send.producer_id()].messages[send.sequence_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ProducerData& producerData = state->producers[send.producer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %" G_GINT64_MODIFIER "u",
                            producerData.producerName.c_str(), send.sequence_id());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                      send.producer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_sequence_id, tvb, cmdOffset, cmdSize,
                                      send.sequence_id());

                // Decode message metadata
                dissect_message_metadata(cmd_tree, tvb, offset, maxOffset);

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                                                  producerData.producerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             producerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                // Pair with frame information
                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::SEND_RECEIPT: {
            const CommandSendReceipt& send_receipt = command.send_receipt();
            RequestData& reqData =
                state->producers[send_receipt.producer_id()].messages[send_receipt.sequence_id()];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ProducerData& producerData = state->producers[send_receipt.producer_id()];
            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %" G_GINT64_MODIFIER "u",
                            producerData.producerName.c_str(), send_receipt.sequence_id());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                      send_receipt.producer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_sequence_id, tvb, cmdOffset, cmdSize,
                                      send_receipt.sequence_id());
                if (send_receipt.has_message_id()) {
                    const MessageIdData& messageId = send_receipt.message_id();
                    proto_tree_add_string_format(cmd_tree, hf_pulsar_message_id, tvb, cmdOffset, cmdSize, "",
                                                 "Message Id: %" G_GINT64_MODIFIER "u:%" G_GINT64_MODIFIER
                                                 "u",
                                                 messageId.ledgerid(), messageId.entryid());
                }

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                                                  producerData.producerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             producerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::SEND_ERROR: {
            const CommandSendError& send_error = command.send_error();
            RequestData& reqData =
                state->producers[send_error.producer_id()].messages[send_error.sequence_id()];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ProducerData& producerData = state->producers[send_error.producer_id()];
            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %" G_GINT64_MODIFIER "u",
                            producerData.producerName.c_str(), send_error.sequence_id());

            if (tree) {
                proto_tree_add_boolean_format(frame_tree, hf_pulsar_error, tvb, cmdOffset, cmdSize, true,
                                              "Error in sending operation");
                proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                      send_error.producer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_sequence_id, tvb, cmdOffset, cmdSize,
                                      send_error.sequence_id());

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_server_error, tvb, cmdOffset, cmdSize,
                                                  to_str(send_error.error(), server_errors_vs));

                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                                             producerData.producerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             producerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                // Pair with frame information
                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::MESSAGE: {
            const CommandMessage& message = command.message();
            state->consumers[message.consumer_id()].messages[message.message_id()];
            RequestData& reqData = state->consumers[message.consumer_id()].messages[message.message_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            const ConsumerData& consumerData = state->consumers[message.consumer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %" G_GINT64_MODIFIER "u:%" G_GINT64_MODIFIER "u",
                            consumerData.consumerName.c_str(), message.message_id().ledgerid(),
                            message.message_id().entryid());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      message.consumer_id());

                dissect_message_metadata(cmd_tree, tvb, offset, maxOffset);

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                                                  consumerData.consumerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             consumerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                // Pair with frame information
                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::ACK: {
            const CommandAck& ack = command.ack();
            RequestData& reqData = state->consumers[ack.consumer_id()].messages[ack.message_id().Get(0)];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            const ConsumerData& consumerData = state->consumers[ack.consumer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %" G_GINT64_MODIFIER "u:%" G_GINT64_MODIFIER "u",
                            consumerData.consumerName.c_str(), ack.message_id().Get(0).ledgerid(),
                            ack.message_id().Get(0).entryid());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      ack.consumer_id());
                proto_tree_add_string(cmd_tree, hf_pulsar_ack_type, tvb, cmdOffset, cmdSize,
                                      to_str(ack.ack_type(), ack_type_vs));

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                                                  consumerData.consumerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             consumerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                // Pair with frame information
                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::FLOW: {
            const CommandFlow& flow = command.flow();
            const ConsumerData& consumerData = state->consumers[flow.consumer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %d", consumerData.consumerName.c_str(),
                            flow.messagepermits());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      flow.consumer_id());
                proto_tree_add_uint(cmd_tree, hf_pulsar_message_permits, tvb, cmdOffset, cmdSize,
                                    flow.messagepermits());

                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                                                  consumerData.consumerName.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             consumerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);
            }
            break;
        }
        case BaseCommand::UNSUBSCRIBE: {
            const CommandUnsubscribe& unsubscribe = command.unsubscribe();
            RequestData& reqData = state->requests[unsubscribe.request_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ConsumerData& consumerData = state->consumers[unsubscribe.consumer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %s / %s / %s",
                            to_str(consumerData.subType, sub_type_names_vs), consumerData.topic.c_str(),
                            consumerData.subscriptionName.c_str(), consumerData.consumerName.c_str());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      unsubscribe.consumer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      unsubscribe.request_id());
                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                                  consumerData.topic.c_str());
                PROTO_ITEM_IS_GENERATED(item);
                item = proto_tree_add_string(cmd_tree, hf_pulsar_subscription, tvb, cmdOffset, cmdSize,
                                             consumerData.subscriptionName.c_str());
                PROTO_ITEM_IS_GENERATED(item);
                proto_tree_add_string(cmd_tree, hf_pulsar_subType, tvb, cmdOffset, cmdSize,
                                      to_str(consumerData.subType, sub_type_names_vs));
                PROTO_ITEM_IS_GENERATED(item);

                proto_tree_add_string(cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                                      consumerData.consumerName.c_str());
                PROTO_ITEM_IS_GENERATED(item);

                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }

        case BaseCommand::SUCCESS: {
            const CommandSuccess& success = command.success();
            RequestResponseData& reqData = state->requests[success.request_id()];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      success.request_id());

                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::ERROR: {
            const CommandError& error = command.error();
            RequestResponseData& reqData = state->requests[error.request_id()];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            if (tree) {
                proto_tree_add_boolean_format(frame_tree, hf_pulsar_error, tvb, cmdOffset, cmdSize, true,
                                              "Request failed");
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      error.request_id());
                proto_tree_add_string(cmd_tree, hf_pulsar_server_error, tvb, cmdOffset, cmdSize,
                                      to_str(error.error(), server_errors_vs));
                proto_tree_add_string(cmd_tree, hf_pulsar_error_message, tvb, cmdOffset, cmdSize,
                                      error.message().c_str());

                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::CLOSE_PRODUCER: {
            const CommandCloseProducer& close_producer = command.close_producer();
            RequestData& reqData = state->requests[close_producer.request_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ProducerData& producerData = state->producers[close_producer.producer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s", producerData.topic.c_str());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                      close_producer.producer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      close_producer.request_id());
                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                                  producerData.topic.c_str());
                PROTO_ITEM_IS_GENERATED(item);

                proto_tree_add_string(cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                                      producerData.producerName.c_str());
                PROTO_ITEM_IS_GENERATED(item);

                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }

        case BaseCommand::CLOSE_CONSUMER: {
            const CommandCloseConsumer& close_consumer = command.close_consumer();
            RequestData& reqData = state->requests[close_consumer.request_id()];
            reqData.requestFrame = pinfo->fd->num;
            reqData.requestTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.requestTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;

            ConsumerData& consumerData = state->consumers[close_consumer.consumer_id()];

            col_append_fstr(pinfo->cinfo, COL_INFO, " / %s / %s / %s / %s",
                            to_str(consumerData.subType, sub_type_names_vs), consumerData.topic.c_str(),
                            consumerData.subscriptionName.c_str(), consumerData.consumerName.c_str());

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_consumer_id, tvb, cmdOffset, cmdSize,
                                      close_consumer.consumer_id());
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      close_consumer.request_id());
                auto item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                                  consumerData.topic.c_str());
                PROTO_ITEM_IS_GENERATED(item);
                item = proto_tree_add_string(cmd_tree, hf_pulsar_subscription, tvb, cmdOffset, cmdSize,
                                             consumerData.subscriptionName.c_str());
                PROTO_ITEM_IS_GENERATED(item);
                proto_tree_add_string(cmd_tree, hf_pulsar_subType, tvb, cmdOffset, cmdSize,
                                      to_str(consumerData.subType, sub_type_names_vs));
                PROTO_ITEM_IS_GENERATED(item);

                proto_tree_add_string(cmd_tree, hf_pulsar_consumer_name, tvb, cmdOffset, cmdSize,
                                      consumerData.consumerName.c_str());
                PROTO_ITEM_IS_GENERATED(item);

                link_to_response_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }

        case BaseCommand::PRODUCER_SUCCESS: {
            const CommandProducerSuccess& success = command.producer_success();
            RequestResponseData& reqData = state->requests[success.request_id()];
            reqData.ackFrame = pinfo->fd->num;
            reqData.ackTimestamp.secs = pinfo->fd->abs_ts.secs;
            reqData.ackTimestamp.nsecs = pinfo->fd->abs_ts.nsecs;
            uint64_t producerId = reqData.id;
            ProducerData& producerData = state->producers[producerId];
            producerData.producerName = success.producer_name();

            if (tree) {
                proto_tree_add_uint64(cmd_tree, hf_pulsar_request_id, tvb, cmdOffset, cmdSize,
                                      success.request_id());
                proto_tree_add_string(cmd_tree, hf_pulsar_producer_name, tvb, cmdOffset, cmdSize,
                                      success.producer_name().c_str());

                auto item = proto_tree_add_uint64(cmd_tree, hf_pulsar_producer_id, tvb, cmdOffset, cmdSize,
                                                  producerId);
                PROTO_ITEM_SET_GENERATED(item);

                item = proto_tree_add_string(cmd_tree, hf_pulsar_topic, tvb, cmdOffset, cmdSize,
                                             producerData.topic.c_str());
                PROTO_ITEM_SET_GENERATED(item);

                link_to_request_frame(cmd_tree, tvb, cmdOffset, cmdSize, reqData);
            }
            break;
        }
        case BaseCommand::PING:
            break;
        case BaseCommand::PONG:
            break;
        case BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES:
            break;
        case BaseCommand::PARTITIONED_METADATA:
            break;
        case BaseCommand::PARTITIONED_METADATA_RESPONSE:
            break;
        case BaseCommand::LOOKUP:
            break;
        case BaseCommand::LOOKUP_RESPONSE:
            break;
        case BaseCommand::CONSUMER_STATS:
            break;
        case BaseCommand::CONSUMER_STATS_RESPONSE:
            break;
        case BaseCommand::REACHED_END_OF_TOPIC:
            break;
        case BaseCommand::SEEK:
            break;
        case BaseCommand::GET_LAST_MESSAGE_ID:
            break;
        case BaseCommand::GET_LAST_MESSAGE_ID_RESPONSE:
            break;
        case BaseCommand::ACTIVE_CONSUMER_CHANGE:
            break;
        case BaseCommand::GET_TOPICS_OF_NAMESPACE:
            break;
        case BaseCommand::GET_TOPICS_OF_NAMESPACE_RESPONSE:
            break;
        case BaseCommand::GET_SCHEMA:
            break;
        case BaseCommand::GET_SCHEMA_RESPONSE:
            break;
        case BaseCommand::AUTH_CHALLENGE:
            break;
        case BaseCommand::AUTH_RESPONSE:
            break;
        case BaseCommand::ACK_RESPONSE:
            break;
        case BaseCommand::GET_OR_CREATE_SCHEMA:
            break;
        case BaseCommand::GET_OR_CREATE_SCHEMA_RESPONSE:
            break;
        case BaseCommand::NEW_TXN:
            break;
        case BaseCommand::NEW_TXN_RESPONSE:
            break;
        case BaseCommand::ADD_PARTITION_TO_TXN:
            break;
        case BaseCommand::ADD_PARTITION_TO_TXN_RESPONSE:
            break;
        case BaseCommand::ADD_SUBSCRIPTION_TO_TXN:
            break;
        case BaseCommand::ADD_SUBSCRIPTION_TO_TXN_RESPONSE:
            break;
        case BaseCommand::END_TXN:
            break;
        case BaseCommand::END_TXN_RESPONSE:
            break;
        case BaseCommand::END_TXN_ON_PARTITION:
            break;
        case BaseCommand::END_TXN_ON_PARTITION_RESPONSE:
            break;
        case BaseCommand::END_TXN_ON_SUBSCRIPTION:
            break;
        case BaseCommand::END_TXN_ON_SUBSCRIPTION_RESPONSE:
            break;
        case BaseCommand::TC_CLIENT_CONNECT_REQUEST:
            break;
        case BaseCommand::TC_CLIENT_CONNECT_RESPONSE:
            break;
    }

    return maxOffset;
}

/* determine PDU length of protocol Pulsar */
static uint32_t get_pulsar_message_len(packet_info* pinfo _U_, tvbuff_t* tvb, int offset, void* data _U_) {
    auto len = (uint32_t)tvb_get_ntohl(tvb, offset);
    return FRAME_SIZE_LEN + len;
}

static int dissect_pulsar(tvbuff_t* tvb, packet_info* pinfo, proto_tree* tree, void* data _U_) {
    tcp_dissect_pdus(tvb, pinfo, tree, 1, FRAME_SIZE_LEN, get_pulsar_message_len, dissect_pulsar_message,
                     data);
    return tvb_captured_length(tvb);
}

static hf_register_info hf[] = {
    //
    {&hf_pulsar_error, {"Error", "apache.pulsar.error", FT_BOOLEAN, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_error_message,
     {"Message", "apache.pulsar.error_message", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_cmd_type,
     {"Command Type", "apache.pulsar.cmd.type", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_frame_size,
     {"Frame size", "apache.pulsar.frame_size", FT_UINT32, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_cmd_size,
     {"Command size", "apache.pulsar.cmd_size", FT_UINT32, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_client_version,
     {"Client version", "apache.pulsar.client_version", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_auth_method,
     {"Auth method", "apache.pulsar.auth_method", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_auth_data,
     {"Auth data", "apache.pulsar.auth_data", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_protocol_version,
     {"Protocol version", "apache.pulsar.protocol_version", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},

    {&hf_pulsar_server_version,
     {"Server version", "apache.pulsar.server_version", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_topic, {"Topic", "apache.pulsar.topic", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_subscription,
     {"Subscription", "apache.pulsar.subscription", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_subType,
     {"Subscription type:", "apache.pulsar.sub_type", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_consumer_id,
     {"Consumer Id", "apache.pulsar.consumer_id", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_producer_id,
     {"Producer Id", "apache.pulsar.producer_id", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_server_error,
     {"Server error", "apache.pulsar.server_error", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},               //
    {&hf_pulsar_ack_type, {"Ack type", "apache.pulsar.ack_type", FT_STRING, 0, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_request_id,
     {"Request Id", "apache.pulsar.request_id", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_consumer_name,
     {"Consumer Name", "apache.pulsar.consumer_name", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_producer_name,
     {"Producer Name", "apache.pulsar.producer_name", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_sequence_id,
     {"Sequence Id", "apache.pulsar.sequence_id", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_highest_sequence_id,
     {"Highest Sequence Id", "apache.pulsar.highest_sequence_id", FT_UINT64, BASE_DEC, NULL, 0x0, NULL,
      HFILL}},                                                                                        //
    {&hf_pulsar_uuid, {"UUID", "apache.pulsar.uuid", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_message_id,
     {"Message Id", "apache.pulsar.message_id", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_message_permits,
     {"Message Permits", "apache.pulsar.message_permits", FT_UINT32, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_publish_time,
     {"Publish Time", "apache.pulsar.publish_time", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_deliver_at_time,
     {"Deliver At Time", "apache.pulsar.deliver_at_time", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_event_time,
     {"Event Time", "apache.pulsar.event_time", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_deliver_after_time,
     {"Deliver After Time", "apache.pulsar.deliver_after_time", FT_UINT64, BASE_DEC, NULL, 0x0, NULL,
      HFILL}},  //

    {&hf_pulsar_chunk_id,
     {"Chunk Id", "apache.pulsar.chunk_id", FT_UINT32, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_num_chunks_from_msg,
     {"Num Chunks From Msg", "apache.pulsar.num_chunks_from_msg", FT_UINT32, BASE_DEC, NULL, 0x0, NULL,
      HFILL}},  //
    {&hf_pulsar_compression_type,
     {"Compression Type", "apache.pulsar.compression_type", FT_STRING, BASE_NONE, NULL, 0x0, NULL,
      HFILL}},  //
    {&hf_pulsar_uncompressed_size,
     {"UnCompression Size", "apache.pulsar.uncompressed_size", FT_UINT32, BASE_DEC, NULL, 0x0, NULL,
      HFILL}},  //

    {&hf_pulsar_replicated_from,
     {"Replicated from", "apache.pulsar.replicated_from", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_partition_key,
     {"Partition Key", "apache.pulsar.partition_key", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_ordering_key,
     {"Ordering Key", "apache.pulsar.ordering_key", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_encryption_algo,
     {"Encryption Algo", "apache.pulsar.encryption_algo", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_encryption_param,
     {"Encryption Param", "apache.pulsar.encryption_param", FT_STRING, BASE_NONE, NULL, 0x0, NULL,
      HFILL}},  //

    {&hf_pulsar_replicate_to,
     {"Replicate to", "apache.pulsar.replicate_to", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_property,
     {"Property", "apache.pulsar.property", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_encryption_keys,
     {"Encryption Keys", "apache.pulsar.encryption_keys", FT_STRING, BASE_NONE, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_txnid_least_bits,
     {"TxnId Least Bits", "apache.pulsar.txnid_least_bits", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //
    {&hf_pulsar_txnid_most_bits,
     {"TxnId Most Bits", "apache.pulsar.txnid_most_bits", FT_UINT64, BASE_DEC, NULL, 0x0, NULL, HFILL}},  //

    {&hf_pulsar_request_in,
     {"Request in frame", "apache.pulsar.request_in", FT_FRAMENUM, BASE_NONE, NULL, 0,
      "This packet is a response to the packet with this number", HFILL}},  //
    {&hf_pulsar_response_in,
     {"Response in frame", "apache.pulsar.response_in", FT_FRAMENUM, BASE_NONE, NULL, 0,
      "This packet will be responded in the packet with this number", HFILL}},  //
    {&hf_pulsar_publish_latency,
     {"Latency", "apache.pulsar.publish_latency", FT_RELATIVE_TIME, BASE_NONE, NULL, 0x0,
      "How long time it took to ACK message", HFILL}},
};

////////////////
///
void proto_register_pulsar() {
    // register the new protocol, protocol fields, and subtrees
    static dissector_handle_t pulsar_handle;

    proto_pulsar = proto_register_protocol("Pulsar Wire Protocol", /* name       */
                                           "Apache Pulsar",        /* short name */
                                           "apache.pulsar"         /* abbrev     */
    );

    /* Setup protocol subtree array */
    static int* ett[] = {&ett_pulsar};

    proto_register_field_array(proto_pulsar, hf, array_length(hf));
    proto_register_subtree_array(ett, array_length(ett));

    pulsar_handle = create_dissector_handle(&dissect_pulsar, proto_pulsar);
    dissector_add_uint("tcp.port", PULSAR_PORT, pulsar_handle);
    register_postdissector(pulsar_handle);
}

extern "C" {

extern __attribute__((unused)) WS_DLL_PUBLIC_DEF const gchar plugin_version[] = VERSION;
extern __attribute__((unused)) WS_DLL_PUBLIC_DEF const int plugin_want_major = VERSION_MAJOR;
extern __attribute__((unused)) WS_DLL_PUBLIC_DEF const int plugin_want_minor = VERSION_MINOR;

WS_DLL_PUBLIC void plugin_register(void);

__attribute__((unused)) static void proto_reg_handoff_pulsar(void) {}

/* Start the functions we need for the plugin stuff */
void plugin_register(void) {
    static proto_plugin plug;
    plug.register_protoinfo = proto_register_pulsar;
    plug.register_handoff = proto_reg_handoff_pulsar; /* or nullptr */
    proto_register_plugin(&plug);
}
}
