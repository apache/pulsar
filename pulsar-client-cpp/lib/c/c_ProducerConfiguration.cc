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
#include <pulsar/c/producer_configuration.h>
#include <include/pulsar/c/message.h>

#include "c_structs.h"

pulsar_producer_configuration_t *pulsar_producer_configuration_create() {
    pulsar_producer_configuration_t *c_conf = new pulsar_producer_configuration_t;
    c_conf->conf = pulsar::ProducerConfiguration();
    return c_conf;
}

void pulsar_producer_configuration_free(pulsar_producer_configuration_t *conf) { delete conf; }

void pulsar_producer_configuration_set_producer_name(pulsar_producer_configuration_t *conf,
                                                     const char *producerName) {
    conf->conf.setProducerName(producerName);
}

const char *pulsar_producer_configuration_get_producer_name(pulsar_producer_configuration_t *conf) {
    return conf->conf.getProducerName().c_str();
}

void pulsar_producer_configuration_set_send_timeout(pulsar_producer_configuration_t *conf,
                                                    int sendTimeoutMs) {
    conf->conf.setSendTimeout(sendTimeoutMs);
}

int pulsar_producer_configuration_get_send_timeout(pulsar_producer_configuration_t *conf) {
    return conf->conf.getSendTimeout();
}

void pulsar_producer_configuration_set_initial_sequence_id(pulsar_producer_configuration_t *conf,
                                                           int64_t initialSequenceId) {
    conf->conf.setInitialSequenceId(initialSequenceId);
}

int64_t pulsar_producer_configuration_get_initial_sequence_id(pulsar_producer_configuration_t *conf) {
    return conf->conf.getInitialSequenceId();
}

void pulsar_producer_configuration_set_compression_type(pulsar_producer_configuration_t *conf,
                                                        pulsar_compression_type compressionType) {
    conf->conf.setCompressionType((pulsar::CompressionType)compressionType);
}

pulsar_compression_type pulsar_producer_configuration_get_compression_type(
    pulsar_producer_configuration_t *conf) {
    return (pulsar_compression_type)conf->conf.getCompressionType();
}

void pulsar_producer_configuration_set_schema_info(pulsar_producer_configuration_t *conf,
                                                   pulsar_schema_type schemaType, const char *name,
                                                   const char *schema, pulsar_string_map_t *properties) {
    auto schemaInfo = pulsar::SchemaInfo((pulsar::SchemaType)schemaType, name, schema, properties->map);
    conf->conf.setSchema(schemaInfo);
}

void pulsar_producer_configuration_set_max_pending_messages(pulsar_producer_configuration_t *conf,
                                                            int maxPendingMessages) {
    conf->conf.setMaxPendingMessages(maxPendingMessages);
}

int pulsar_producer_configuration_get_max_pending_messages(pulsar_producer_configuration_t *conf) {
    return conf->conf.getMaxPendingMessages();
}

void pulsar_producer_configuration_set_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf, int maxPendingMessagesAcrossPartitions) {
    conf->conf.setMaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions);
}

int pulsar_producer_configuration_get_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf) {
    return conf->conf.getMaxPendingMessagesAcrossPartitions();
}

void pulsar_producer_configuration_set_partitions_routing_mode(pulsar_producer_configuration_t *conf,
                                                               pulsar_partitions_routing_mode mode) {
    conf->conf.setPartitionsRoutingMode((pulsar::ProducerConfiguration::PartitionsRoutingMode)mode);
}

pulsar_partitions_routing_mode pulsar_producer_configuration_get_partitions_routing_mode(
    pulsar_producer_configuration_t *conf) {
    return (pulsar_partitions_routing_mode)conf->conf.getPartitionsRoutingMode();
}

void pulsar_producer_configuration_set_hashing_scheme(pulsar_producer_configuration_t *conf,
                                                      pulsar_hashing_scheme scheme) {
    conf->conf.setHashingScheme((pulsar::ProducerConfiguration::HashingScheme)scheme);
}

pulsar_hashing_scheme pulsar_producer_configuration_get_hashing_scheme(
    pulsar_producer_configuration_t *conf) {
    return (pulsar_hashing_scheme)conf->conf.getHashingScheme();
}

class MessageRoutingPolicy : public pulsar::MessageRoutingPolicy {
    pulsar_message_router _router;
    void *_ctx;

   public:
    MessageRoutingPolicy(pulsar_message_router router, void *ctx) : _router(router), _ctx(ctx) {}

    int getPartition(const pulsar::Message &msg, const pulsar::TopicMetadata &topicMetadata) {
        pulsar_message_t message;
        message.message = msg;

        pulsar_topic_metadata_t metadata;
        metadata.metadata = &topicMetadata;

        return _router(&message, &metadata, _ctx);
    }
};

void pulsar_producer_configuration_set_message_router(pulsar_producer_configuration_t *conf,
                                                      pulsar_message_router router, void *ctx) {
    conf->conf.setMessageRouter(std::make_shared<MessageRoutingPolicy>(router, ctx));
}

void pulsar_producer_configuration_set_block_if_queue_full(pulsar_producer_configuration_t *conf,
                                                           int blockIfQueueFull) {
    conf->conf.setBlockIfQueueFull(blockIfQueueFull);
}

int pulsar_producer_configuration_get_block_if_queue_full(pulsar_producer_configuration_t *conf) {
    return conf->conf.getBlockIfQueueFull();
}

void pulsar_producer_configuration_set_batching_enabled(pulsar_producer_configuration_t *conf,
                                                        int batchingEnabled) {
    conf->conf.setBatchingEnabled(batchingEnabled);
}

int pulsar_producer_configuration_get_batching_enabled(pulsar_producer_configuration_t *conf) {
    return conf->conf.getBatchingEnabled();
}

void pulsar_producer_configuration_set_batching_max_messages(pulsar_producer_configuration_t *conf,
                                                             unsigned int batchingMaxMessages) {
    conf->conf.setBatchingMaxMessages(batchingMaxMessages);
}

unsigned int pulsar_producer_configuration_get_batching_max_messages(pulsar_producer_configuration_t *conf) {
    return conf->conf.getBatchingMaxMessages();
}

void pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf, unsigned long batchingMaxAllowedSizeInBytes) {
    conf->conf.setBatchingMaxAllowedSizeInBytes(batchingMaxAllowedSizeInBytes);
}

unsigned long pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf) {
    return conf->conf.getBatchingMaxAllowedSizeInBytes();
}

void pulsar_producer_configuration_set_batching_max_publish_delay_ms(
    pulsar_producer_configuration_t *conf, unsigned long batchingMaxPublishDelayMs) {
    conf->conf.setBatchingMaxPublishDelayMs(batchingMaxPublishDelayMs);
}

unsigned long pulsar_producer_configuration_get_batching_max_publish_delay_ms(
    pulsar_producer_configuration_t *conf) {
    return conf->conf.getBatchingMaxPublishDelayMs();
}

void pulsar_producer_configuration_set_property(pulsar_producer_configuration_t *conf, const char *name,
                                                const char *value) {
    conf->conf.setProperty(name, value);
}
