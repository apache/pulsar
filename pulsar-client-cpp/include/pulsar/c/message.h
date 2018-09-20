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

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#include "string_map.h"

#pragma GCC visibility push(default)

typedef struct _pulsar_message pulsar_message_t;
typedef struct _pulsar_message_id pulsar_message_id_t;

pulsar_message_t *pulsar_message_create();
void pulsar_message_free(pulsar_message_t *message);

/// Builder

void pulsar_message_set_content(pulsar_message_t *message, const void *data, size_t size);

/**
 * Set content of the message to a buffer already allocated by the caller. No copies of
 * this buffer will be made. The caller is responsible to ensure the memory buffer is
 * valid until the message has been persisted (or an error is returned).
 */
void pulsar_message_set_allocated_content(pulsar_message_t *message, void *data, size_t size);

void pulsar_message_set_property(pulsar_message_t *message, const char *name, const char *value);

/*
 * set partition key for the message routing
 * @param hash of this key is used to determine message's topic partition
 */
void pulsar_message_set_partition_key(pulsar_message_t *message, const char *partitionKey);

/**
 * Set the event timestamp for the message.
 */
void pulsar_message_set_event_timestamp(pulsar_message_t *message, uint64_t eventTimestamp);

/**
 * Specify a custom sequence id for the message being published.
 * <p>
 * The sequence id can be used for deduplication purposes and it needs to follow these rules:
 * <ol>
 * <li><code>sequenceId >= 0</code>
 * <li>Sequence id for a message needs to be greater than sequence id for earlier messages:
 * <code>sequenceId(N+1) > sequenceId(N)</code>
 * <li>It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
 * <code>sequenceId</code> could represent an offset or a cumulative size.
 * </ol>
 *
 * @param sequenceId
 *            the sequence id to assign to the current message
 */
void pulsar_message_set_sequence_id(pulsar_message_t *message, int64_t sequenceId);

/**
 * override namespace replication clusters.  note that it is the
 * caller's responsibility to provide valid cluster names, and that
 * all clusters have been previously configured as topics.
 *
 * given an empty list, the message will replicate per the namespace
 * configuration.
 *
 * @param clusters where to send this message.
 */
void pulsar_message_set_replication_clusters(pulsar_message_t *message, const char **clusters);

/**
 * Do not replicate this message
 * @param flag if true, disable replication, otherwise use default
 * replication
 */
void pulsar_message_disable_replication(pulsar_message_t *message, int flag);

/// Accessor for built messages

/**
 * Return the properties attached to the message.
 * Properties are application defined key/value pairs that will be attached to the message
 *
 * @return an unmodifiable view of the properties map
 */
pulsar_string_map_t *pulsar_message_get_properties(pulsar_message_t *message);

/**
 * Check whether the message has a specific property attached.
 *
 * @param name the name of the property to check
 * @return true if the message has the specified property
 * @return false if the property is not defined
 */
int pulsar_message_has_property(pulsar_message_t *message, const char *name);

/**
 * Get the value of a specific property
 *
 * @param name the name of the property
 * @return the value of the property or null if the property was not defined
 */
const char *pulsar_message_get_property(pulsar_message_t *message, const char *name);

/**
 * Get the content of the message
 *
 *
 * @return the pointer to the message payload
 */
const void *pulsar_message_get_data(pulsar_message_t *message);

/**
 * Get the length of the message
 *
 * @return the length of the message payload
 */
uint32_t pulsar_message_get_length(pulsar_message_t *message);

/**
 * Get the unique message ID associated with this message.
 *
 * The message id can be used to univocally refer to a message without having to keep the entire payload
 * in memory.
 *
 * Only messages received from the consumer will have a message id assigned.
 *
 */
pulsar_message_id_t *pulsar_message_get_message_id(pulsar_message_t *message);

/**
 * Get the partition key for this message
 * @return key string that is hashed to determine message's topic partition
 */
const char *pulsar_message_get_partitionKey(pulsar_message_t *message);
int pulsar_message_has_partition_key(pulsar_message_t *message);

/**
 * Get the UTC based timestamp in milliseconds referring to when the message was published by the client
 * producer
 */
uint64_t pulsar_message_get_publish_timestamp(pulsar_message_t *message);

/**
 * Get the event timestamp associated with this message. It is set by the client producer.
 */
uint64_t pulsar_message_get_event_timestamp(pulsar_message_t *message);

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif