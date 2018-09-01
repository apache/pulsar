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

#include <stdint.h>

#include <pulsar/c/message_router.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma GCC visibility push(default)

typedef enum {
    pulsar_UseSinglePartition,
    pulsar_RoundRobinDistribution,
    pulsar_CustomPartition
} pulsar_partitions_routing_mode;

typedef enum { pulsar_Murmur3_32Hash, pulsar_BoostHash, pulsar_JavaStringHash } pulsar_hashing_scheme;

typedef enum {
    pulsar_CompressionNone = 0,
    pulsar_CompressionLZ4 = 1,
    pulsar_CompressionZLib = 2
} pulsar_compression_type;

typedef struct _pulsar_producer_configuration pulsar_producer_configuration_t;

pulsar_producer_configuration_t *pulsar_producer_configuration_create();

void pulsar_producer_configuration_free(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_producer_name(pulsar_producer_configuration_t *conf,
                                                     const char *producerName);

const char *pulsar_producer_configuration_get_producer_name(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_send_timeout(pulsar_producer_configuration_t *conf, int sendTimeoutMs);

int pulsar_producer_configuration_get_send_timeout(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_initial_sequence_id(pulsar_producer_configuration_t *conf,
                                                           int64_t initialSequenceId);

int64_t pulsar_producer_configuration_get_initial_sequence_id(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_compression_type(pulsar_producer_configuration_t *conf,
                                                        pulsar_compression_type compressionType);

pulsar_compression_type pulsar_producer_configuration_get_compression_type(
    pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_max_pending_messages(pulsar_producer_configuration_t *conf,
                                                            int maxPendingMessages);

int pulsar_producer_configuration_get_max_pending_messages(pulsar_producer_configuration_t *conf);

/**
 * Set the number of max pending messages across all the partitions
 * <p>
 * This setting will be used to lower the max pending messages for each partition
 * ({@link #setMaxPendingMessages(int)}), if the total exceeds the configured value.
 *
 * @param maxPendingMessagesAcrossPartitions
 */
void pulsar_producer_configuration_set_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf, int maxPendingMessagesAcrossPartitions);

/**
 *
 * @return the maximum number of pending messages allowed across all the partitions
 */
int pulsar_producer_configuration_get_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_partitions_routing_mode(pulsar_producer_configuration_t *conf,
                                                               pulsar_partitions_routing_mode mode);

pulsar_partitions_routing_mode pulsar_producer_configuration_get_partitions_routing_mode(
    pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_message_router(pulsar_producer_configuration_t *conf,
                                                      pulsar_message_router router, void *ctx);

void pulsar_producer_configuration_set_hashing_scheme(pulsar_producer_configuration_t *conf,
                                                      pulsar_hashing_scheme scheme);

pulsar_hashing_scheme pulsar_producer_configuration_get_hashing_scheme(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_block_if_queue_full(pulsar_producer_configuration_t *conf,
                                                           int blockIfQueueFull);

int pulsar_producer_configuration_get_block_if_queue_full(pulsar_producer_configuration_t *conf);

// Zero queue size feature will not be supported on consumer end if batching is enabled
void pulsar_producer_configuration_set_batching_enabled(pulsar_producer_configuration_t *conf,
                                                        int batchingEnabled);

int pulsar_producer_configuration_get_batching_enabled(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_batching_max_messages(pulsar_producer_configuration_t *conf,
                                                             unsigned int batchingMaxMessages);

unsigned int pulsar_producer_configuration_get_batching_max_messages(pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf, unsigned long batchingMaxAllowedSizeInBytes);

unsigned long pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_batching_max_publish_delay_ms(pulsar_producer_configuration_t *conf,
                                                                     unsigned long batchingMaxPublishDelayMs);

unsigned long pulsar_producer_configuration_get_batching_max_publish_delay_ms(
    pulsar_producer_configuration_t *conf);

void pulsar_producer_configuration_set_property(pulsar_producer_configuration_t *conf, const char *name,
                                                const char *value);

// const CryptoKeyReaderPtr getCryptoKeyReader() const;
// ProducerConfiguration &setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);
//
// ProducerCryptoFailureAction getCryptoFailureAction() const;
// ProducerConfiguration &setCryptoFailureAction(ProducerCryptoFailureAction action);
//
// std::set <std::string> &getEncryptionKeys();
// int isEncryptionEnabled() const;
// ProducerConfiguration &addEncryptionKey(std::string key);

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif