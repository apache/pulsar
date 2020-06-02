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

#include <pulsar/c/message.h>
#include "c_structs.h"

pulsar_message_t *pulsar_message_create() { return new pulsar_message_t; }

void pulsar_message_free(pulsar_message_t *message) { delete message; }

void pulsar_message_set_content(pulsar_message_t *message, const void *data, size_t size) {
    message->builder.setContent(data, size);
}

void pulsar_message_set_allocated_content(pulsar_message_t *message, void *data, size_t size) {
    message->builder.setAllocatedContent(data, size);
}

void pulsar_message_set_property(pulsar_message_t *message, const char *name, const char *value) {
    message->builder.setProperty(name, value);
}

void pulsar_message_set_partition_key(pulsar_message_t *message, const char *partitionKey) {
    message->builder.setPartitionKey(partitionKey);
}

void pulsar_message_set_ordering_key(pulsar_message_t *message, const char *orderingKey) {
    message->builder.setOrderingKey(orderingKey);
}

void pulsar_message_set_event_timestamp(pulsar_message_t *message, uint64_t eventTimestamp) {
    message->builder.setEventTimestamp(eventTimestamp);
}

void pulsar_message_set_sequence_id(pulsar_message_t *message, int64_t sequenceId) {
    message->builder.setSequenceId(sequenceId);
}

void pulsar_message_set_deliver_after(pulsar_message_t *message, uint64_t delayMillis) {
    message->builder.setDeliverAfter(std::chrono::milliseconds(delayMillis));
}

void pulsar_message_set_deliver_at(pulsar_message_t *message, uint64_t deliveryTimestampMillis) {
    message->builder.setDeliverAt(deliveryTimestampMillis);
}

void pulsar_message_set_replication_clusters(pulsar_message_t *message, const char **clusters, size_t size) {
    const char **c = clusters;
    std::vector<std::string> clustersList;
    for (size_t i = 0; i < size; i++) {
        clustersList.push_back(*c);
        ++c;
    }

    message->builder.setReplicationClusters(clustersList);
}

void pulsar_message_disable_replication(pulsar_message_t *message, int flag) {
    message->builder.disableReplication(flag);
}

int pulsar_message_has_property(pulsar_message_t *message, const char *name) {
    return message->message.hasProperty(name);
}

const char *pulsar_message_get_property(pulsar_message_t *message, const char *name) {
    return message->message.getProperty(name).c_str();
}

const void *pulsar_message_get_data(pulsar_message_t *message) { return message->message.getData(); }

uint32_t pulsar_message_get_length(pulsar_message_t *message) { return message->message.getLength(); }

pulsar_message_id_t *pulsar_message_get_message_id(pulsar_message_t *message) {
    pulsar_message_id_t *messageId = new pulsar_message_id_t;
    messageId->messageId = message->message.getMessageId();
    return messageId;
}

const char *pulsar_message_get_partitionKey(pulsar_message_t *message) {
    return message->message.getPartitionKey().c_str();
}

int pulsar_message_has_partition_key(pulsar_message_t *message) { return message->message.hasPartitionKey(); }

const char *pulsar_message_get_orderingKey(pulsar_message_t *message) {
    return message->message.getOrderingKey().c_str();
}

int pulsar_message_has_ordering_key(pulsar_message_t *message) { return message->message.hasOrderingKey(); }

uint64_t pulsar_message_get_publish_timestamp(pulsar_message_t *message) {
    return message->message.getPublishTimestamp();
}

uint64_t pulsar_message_get_event_timestamp(pulsar_message_t *message) {
    return message->message.getEventTimestamp();
}

pulsar_string_map_t *pulsar_message_get_properties(pulsar_message_t *message) {
    pulsar_string_map_t *map = pulsar_string_map_create();
    map->map = message->message.getProperties();
    return map;
}

const char *pulsar_message_get_topic_name(pulsar_message_t *message) {
    return message->message.getTopicName().c_str();
}

int pulsar_message_get_redelivery_count(pulsar_message_t *message) {
    return message->message.getRedeliveryCount();
}
