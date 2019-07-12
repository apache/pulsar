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

#include <pulsar/c/consumer.h>
#include <pulsar/c/consumer_configuration.h>

#include "c_structs.h"

pulsar_consumer_configuration_t *pulsar_consumer_configuration_create() {
    return new pulsar_consumer_configuration_t;
}

void pulsar_consumer_configuration_free(pulsar_consumer_configuration_t *consumer_configuration) {
    delete consumer_configuration;
}

void pulsar_consumer_configuration_set_consumer_type(pulsar_consumer_configuration_t *consumer_configuration,
                                                     pulsar_consumer_type consumerType) {
    consumer_configuration->consumerConfiguration.setConsumerType((pulsar::ConsumerType)consumerType);
}

pulsar_consumer_type pulsar_consumer_configuration_get_consumer_type(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return (pulsar_consumer_type)consumer_configuration->consumerConfiguration.getConsumerType();
}

void pulsar_consumer_configuration_set_schema_info(pulsar_consumer_configuration_t *consumer_configuration,
                                                   pulsar_schema_type schemaType, const char *name,
                                                   const char *schema, pulsar_string_map_t *properties) {
    auto schemaInfo = pulsar::SchemaInfo((pulsar::SchemaType)schemaType, name, schema, properties->map);
    consumer_configuration->consumerConfiguration.setSchema(schemaInfo);
}

static void message_listener_callback(pulsar::Consumer consumer, const pulsar::Message &msg,
                                      pulsar_message_listener listener, void *ctx) {
    pulsar_consumer_t c_consumer;
    c_consumer.consumer = consumer;
    pulsar_message_t *message = new pulsar_message_t;
    message->message = msg;
    listener(&c_consumer, message, ctx);
}

void pulsar_consumer_configuration_set_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_message_listener messageListener,
    void *ctx) {
    consumer_configuration->consumerConfiguration.setMessageListener(std::bind(
        message_listener_callback, std::placeholders::_1, std::placeholders::_2, messageListener, ctx));
}

int pulsar_consumer_configuration_has_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.hasMessageListener();
}

void pulsar_consumer_configuration_set_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration, int size) {
    consumer_configuration->consumerConfiguration.setReceiverQueueSize(size);
}

int pulsar_consumer_configuration_get_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getReceiverQueueSize();
}

void pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration, int maxTotalReceiverQueueSizeAcrossPartitions) {
    consumer_configuration->consumerConfiguration.setMaxTotalReceiverQueueSizeAcrossPartitions(
        maxTotalReceiverQueueSizeAcrossPartitions);
}

int pulsar_consumer_get_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getMaxTotalReceiverQueueSizeAcrossPartitions();
}

void pulsar_consumer_set_consumer_name(pulsar_consumer_configuration_t *consumer_configuration,
                                       const char *consumerName) {
    consumer_configuration->consumerConfiguration.setConsumerName(consumerName);
}

const char *pulsar_consumer_get_consumer_name(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getConsumerName().c_str();
}

void pulsar_consumer_set_unacked_messages_timeout_ms(pulsar_consumer_configuration_t *consumer_configuration,
                                                     const uint64_t milliSeconds) {
    consumer_configuration->consumerConfiguration.setUnAckedMessagesTimeoutMs(milliSeconds);
}

long pulsar_consumer_get_unacked_messages_timeout_ms(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getUnAckedMessagesTimeoutMs();
}

void pulsar_configure_set_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration, long redeliveryDelayMillis) {
    consumer_configuration->consumerConfiguration.setNegativeAckRedeliveryDelayMs(redeliveryDelayMillis);
}

long pulsar_configure_get_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getNegativeAckRedeliveryDelayMs();
}

int pulsar_consumer_is_encryption_enabled(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isEncryptionEnabled();
}

int pulsar_consumer_is_read_compacted(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isReadCompacted();
}

void pulsar_consumer_set_read_compacted(pulsar_consumer_configuration_t *consumer_configuration,
                                        int compacted) {
    consumer_configuration->consumerConfiguration.setReadCompacted(compacted);
}

void pulsar_consumer_configuration_set_property(pulsar_consumer_configuration_t *conf, const char *name,
                                                const char *value) {
    conf->consumerConfiguration.setProperty(name, value);
}

void pulsar_consumer_set_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration, initial_position subscriptionInitialPosition) {
    consumer_configuration->consumerConfiguration.setSubscriptionInitialPosition(
        (pulsar::InitialPosition)subscriptionInitialPosition);
}

int pulsar_consumer_get_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getSubscriptionInitialPosition();
}
