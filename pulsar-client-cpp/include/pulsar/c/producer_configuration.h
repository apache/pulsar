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

#include <pulsar/defines.h>
#include <pulsar/c/message_router.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    pulsar_UseSinglePartition,
    pulsar_RoundRobinDistribution,
    pulsar_CustomPartition
} pulsar_partitions_routing_mode;

typedef enum { pulsar_Murmur3_32Hash, pulsar_BoostHash, pulsar_JavaStringHash } pulsar_hashing_scheme;

typedef enum {
    pulsar_CompressionNone = 0,
    pulsar_CompressionLZ4 = 1,
    pulsar_CompressionZLib = 2,
    pulsar_CompressionZSTD = 3,
    pulsar_CompressionSNAPPY = 4
} pulsar_compression_type;

typedef enum {
    pulsar_None = 0,
    pulsar_String = 1,
    pulsar_Json = 2,
    pulsar_Protobuf = 3,
    pulsar_Avro = 4,
    pulsar_Boolean = 5,
    pulsar_Int8 = 6,
    pulsar_Int16 = 7,
    pulsar_Int32 = 8,
    pulsar_Int64 = 9,
    pulsar_Float32 = 10,
    pulsar_Float64 = 11,
    pulsar_KeyValue = 15,
    pulsar_Bytes = -1,
    pulsar_AutoConsume = -3,
    pulsar_AutoPublish = -4,
} pulsar_schema_type;

typedef enum {
    // This is the default option to fail send if crypto operation fails
    pulsar_ProducerFail,
    // Ignore crypto failure and proceed with sending unencrypted messages
    pulsar_ProducerSend
} pulsar_producer_crypto_failure_action;

typedef struct _pulsar_producer_configuration pulsar_producer_configuration_t;

typedef struct _pulsar_crypto_key_reader pulsar_crypto_key_reader;

PULSAR_PUBLIC pulsar_producer_configuration_t *pulsar_producer_configuration_create();

PULSAR_PUBLIC void pulsar_producer_configuration_free(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_producer_name(pulsar_producer_configuration_t *conf,
                                                                   const char *producerName);

PULSAR_PUBLIC const char *pulsar_producer_configuration_get_producer_name(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_send_timeout(pulsar_producer_configuration_t *conf,
                                                                  int sendTimeoutMs);

PULSAR_PUBLIC int pulsar_producer_configuration_get_send_timeout(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_initial_sequence_id(
    pulsar_producer_configuration_t *conf, int64_t initialSequenceId);

PULSAR_PUBLIC int64_t
pulsar_producer_configuration_get_initial_sequence_id(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_compression_type(
    pulsar_producer_configuration_t *conf, pulsar_compression_type compressionType);

PULSAR_PUBLIC pulsar_compression_type
pulsar_producer_configuration_get_compression_type(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_schema_info(pulsar_producer_configuration_t *conf,
                                                                 pulsar_schema_type schemaType,
                                                                 const char *name, const char *schema,
                                                                 pulsar_string_map_t *properties);

PULSAR_PUBLIC void pulsar_producer_configuration_set_max_pending_messages(
    pulsar_producer_configuration_t *conf, int maxPendingMessages);
PULSAR_PUBLIC int pulsar_producer_configuration_get_max_pending_messages(
    pulsar_producer_configuration_t *conf);

/**
 * Set the number of max pending messages across all the partitions
 * <p>
 * This setting will be used to lower the max pending messages for each partition
 * ({@link #setMaxPendingMessages(int)}), if the total exceeds the configured value.
 *
 * @param maxPendingMessagesAcrossPartitions
 */
PULSAR_PUBLIC void pulsar_producer_configuration_set_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf, int maxPendingMessagesAcrossPartitions);

/**
 *
 * @return the maximum number of pending messages allowed across all the partitions
 */
PULSAR_PUBLIC int pulsar_producer_configuration_get_max_pending_messages_across_partitions(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_partitions_routing_mode(
    pulsar_producer_configuration_t *conf, pulsar_partitions_routing_mode mode);

PULSAR_PUBLIC pulsar_partitions_routing_mode
pulsar_producer_configuration_get_partitions_routing_mode(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_message_router(pulsar_producer_configuration_t *conf,
                                                                    pulsar_message_router router, void *ctx);

PULSAR_PUBLIC void pulsar_producer_configuration_set_hashing_scheme(pulsar_producer_configuration_t *conf,
                                                                    pulsar_hashing_scheme scheme);

PULSAR_PUBLIC pulsar_hashing_scheme
pulsar_producer_configuration_get_hashing_scheme(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_lazy_start_partitioned_producers(
    pulsar_producer_configuration_t *conf, int useLazyStartPartitionedProducers);

PULSAR_PUBLIC int pulsar_producer_configuration_get_lazy_start_partitioned_producers(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_block_if_queue_full(
    pulsar_producer_configuration_t *conf, int blockIfQueueFull);

PULSAR_PUBLIC int pulsar_producer_configuration_get_block_if_queue_full(
    pulsar_producer_configuration_t *conf);

// Zero queue size feature will not be supported on consumer end if batching is enabled
PULSAR_PUBLIC void pulsar_producer_configuration_set_batching_enabled(pulsar_producer_configuration_t *conf,
                                                                      int batchingEnabled);

PULSAR_PUBLIC int pulsar_producer_configuration_get_batching_enabled(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_batching_max_messages(
    pulsar_producer_configuration_t *conf, unsigned int batchingMaxMessages);

PULSAR_PUBLIC unsigned int pulsar_producer_configuration_get_batching_max_messages(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf, unsigned long batchingMaxAllowedSizeInBytes);

PULSAR_PUBLIC unsigned long pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_batching_max_publish_delay_ms(
    pulsar_producer_configuration_t *conf, unsigned long batchingMaxPublishDelayMs);

PULSAR_PUBLIC unsigned long pulsar_producer_configuration_get_batching_max_publish_delay_ms(
    pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_property(pulsar_producer_configuration_t *conf,
                                                              const char *name, const char *value);

PULSAR_PUBLIC int pulsar_producer_is_encryption_enabled(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_default_crypto_key_reader(
    pulsar_producer_configuration_t *conf, const char *public_key_path, const char *private_key_path);

PULSAR_PUBLIC pulsar_producer_crypto_failure_action
pulsar_producer_configuration_get_crypto_failure_action(pulsar_producer_configuration_t *conf);

PULSAR_PUBLIC void pulsar_producer_configuration_set_crypto_failure_action(
    pulsar_producer_configuration_t *conf, pulsar_producer_crypto_failure_action cryptoFailureAction);

PULSAR_PUBLIC void pulsar_producer_configuration_set_encryption_key(pulsar_producer_configuration_t *conf,
                                                                    const char *key);

// const CryptoKeyReaderPtr getCryptoKeyReader() const;
// ProducerConfiguration &setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);
//
// ProducerCryptoFailureAction getCryptoFailureAction() const;
// ProducerConfiguration &setCryptoFailureAction(ProducerCryptoFailureAction action);
//
// std::set <std::string> &getEncryptionKeys();
// int isEncryptionEnabled() const;
// ProducerConfiguration &addEncryptionKey(std::string key);

#ifdef __cplusplus
}
#endif
