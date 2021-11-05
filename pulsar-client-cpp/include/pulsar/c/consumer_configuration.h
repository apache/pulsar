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

#include <pulsar/defines.h>
#include "consumer.h"
#include "producer_configuration.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_consumer_configuration pulsar_consumer_configuration_t;

typedef enum {
    /**
     * There can be only 1 consumer on the same topic with the same consumerName
     */
    pulsar_ConsumerExclusive,

    /**
     * Multiple consumers will be able to use the same consumerName and the messages
     *  will be dispatched according to a round-robin rotation between the connected consumers
     */
    pulsar_ConsumerShared,

    /** Only one consumer is active on the subscription; Subscription can have N consumers
     *  connected one of which will get promoted to master if the current master becomes inactive
     */
    pulsar_ConsumerFailover,

    /**
     * Multiple consumer will be able to use the same subscription and all messages with the same key
     * will be dispatched to only one consumer
     */
    pulsar_ConsumerKeyShared
} pulsar_consumer_type;

typedef enum {
    /**
     * the latest position which means the start consuming position will be the last message
     */
    initial_position_latest,
    /**
     * the earliest position which means the start consuming position will be the first message
     */
    initial_position_earliest
} initial_position;

typedef enum {
    // This is the default option to fail consume until crypto succeeds
    pulsar_ConsumerFail,
    // Message is silently acknowledged and not delivered to the application
    pulsar_ConsumerDiscard,
    // Deliver the encrypted message to the application. It's the application's
    // responsibility to decrypt the message. If message is also compressed,
    // decompression will fail. If message contain batch messages, client will
    // not be able to retrieve individual messages in the batch
    pulsar_ConsumerConsume
} pulsar_consumer_crypto_failure_action;

/// Callback definition for MessageListener
typedef void (*pulsar_message_listener)(pulsar_consumer_t *consumer, pulsar_message_t *msg, void *ctx);

PULSAR_PUBLIC pulsar_consumer_configuration_t *pulsar_consumer_configuration_create();

PULSAR_PUBLIC void pulsar_consumer_configuration_free(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Specify the consumer type. The consumer type enables
 * specifying the type of subscription. In Exclusive subscription,
 * only a single consumer is allowed to attach to the subscription. Other consumers
 * will get an error message. In Shared subscription, multiple consumers will be
 * able to use the same subscription name and the messages will be dispatched in a
 * round robin fashion. In Failover subscription, a primary-failover subscription model
 * allows for multiple consumers to attach to a single subscription, though only one
 * of them will be “master” at a given time. Only the primary consumer will receive
 * messages. When the primary consumer gets disconnected, one among the failover
 * consumers will be promoted to primary and will start getting messages.
 */
PULSAR_PUBLIC void pulsar_consumer_configuration_set_consumer_type(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_consumer_type consumerType);

PULSAR_PUBLIC pulsar_consumer_type
pulsar_consumer_configuration_get_consumer_type(pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_configuration_set_schema_info(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_schema_type schemaType, const char *name,
    const char *schema, pulsar_string_map_t *properties);

/**
 * A message listener enables your application to configure how to process
 * and acknowledge messages delivered. A listener will be called in order
 * for every message received.
 */
PULSAR_PUBLIC void pulsar_consumer_configuration_set_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_message_listener messageListener,
    void *ctx);

PULSAR_PUBLIC int pulsar_consumer_configuration_has_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Sets the size of the consumer receive queue.
 *
 * The consumer receive queue controls how many messages can be accumulated by the Consumer before the
 * application calls receive(). Using a higher value could potentially increase the consumer throughput
 * at the expense of bigger memory utilization.
 *
 * Setting the consumer queue size as zero decreases the throughput of the consumer, by disabling
 * pre-fetching of
 * messages. This approach improves the message distribution on shared subscription, by pushing messages
 * only to
 * the consumers that are ready to process them. Neither receive with timeout nor Partitioned Topics can
 * be
 * used if the consumer queue size is zero. The receive() function call should not be interrupted when
 * the consumer queue size is zero.
 *
 * Default value is 1000 messages and should be good for most use cases.
 *
 * @param size
 *            the new receiver queue size value
 */
PULSAR_PUBLIC void pulsar_consumer_configuration_set_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration, int size);

PULSAR_PUBLIC int pulsar_consumer_configuration_get_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set the max total receiver queue size across partitons.
 * <p>
 * This setting will be used to reduce the receiver queue size for individual partitions
 * {@link #setReceiverQueueSize(int)} if the total exceeds this value (default: 50000).
 *
 * @param maxTotalReceiverQueueSizeAcrossPartitions
 */
PULSAR_PUBLIC void pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration, int maxTotalReceiverQueueSizeAcrossPartitions);

/**
 * @return the configured max total receiver queue size across partitions
 */
PULSAR_PUBLIC int pulsar_consumer_get_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_set_consumer_name(pulsar_consumer_configuration_t *consumer_configuration,
                                                     const char *consumerName);

PULSAR_PUBLIC const char *pulsar_consumer_get_consumer_name(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set the timeout in milliseconds for unacknowledged messages, the timeout needs to be greater than
 * 10 seconds. An Exception is thrown if the given value is less than 10000 (10 seconds).
 * If a successful acknowledgement is not sent within the timeout all the unacknowledged messages are
 * redelivered.
 * @param timeout in milliseconds
 */
PULSAR_PUBLIC void pulsar_consumer_set_unacked_messages_timeout_ms(
    pulsar_consumer_configuration_t *consumer_configuration, const uint64_t milliSeconds);

/**
 * @return the configured timeout in milliseconds for unacked messages.
 */
PULSAR_PUBLIC long pulsar_consumer_get_unacked_messages_timeout_ms(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set the delay to wait before re-delivering messages that have failed to be process.
 * <p>
 * When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message
 * will be redelivered after a fixed timeout. The default is 1 min.
 *
 * @param redeliveryDelay
 *            redelivery delay for failed messages
 * @param timeUnit
 *            unit in which the timeout is provided.
 * @return the consumer builder instance
 */
PULSAR_PUBLIC void pulsar_configure_set_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration, long redeliveryDelayMillis);

/**
 * Get the configured delay to wait before re-delivering messages that have failed to be process.
 *
 * @param consumer_configuration the consumer conf object
 * @return redelivery delay for failed messages
 */
PULSAR_PUBLIC long pulsar_configure_get_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set time window in milliseconds for grouping message ACK requests. An ACK request is not sent
 * to broker until the time window reaches its end, or the number of grouped messages reaches
 * limit. Default is 100 milliseconds. If it's set to a non-positive value, ACK requests will be
 * directly sent to broker without grouping.
 *
 * @param consumer_configuration the consumer conf object
 * @param ackGroupMillis time of ACK grouping window in milliseconds.
 */
PULSAR_PUBLIC void pulsar_configure_set_ack_grouping_time_ms(
    pulsar_consumer_configuration_t *consumer_configuration, long ackGroupingMillis);

/**
 * Get grouping time window in milliseconds.
 *
 * @param consumer_configuration the consumer conf object
 * @return grouping time window in milliseconds.
 */
PULSAR_PUBLIC long pulsar_configure_get_ack_grouping_time_ms(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set max number of grouped messages within one grouping time window. If it's set to a
 * non-positive value, number of grouped messages is not limited. Default is 1000.
 *
 * @param consumer_configuration the consumer conf object
 * @param maxGroupingSize max number of grouped messages with in one grouping time window.
 */
PULSAR_PUBLIC void pulsar_configure_set_ack_grouping_max_size(
    pulsar_consumer_configuration_t *consumer_configuration, long maxGroupingSize);

/**
 * Get max number of grouped messages within one grouping time window.
 *
 * @param consumer_configuration the consumer conf object
 * @return max number of grouped messages within one grouping time window.
 */
PULSAR_PUBLIC long pulsar_configure_get_ack_grouping_max_size(
    pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC int pulsar_consumer_is_encryption_enabled(
    pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_configuration_set_default_crypto_key_reader(
    pulsar_consumer_configuration_t *consumer_configuration, const char *public_key_path,
    const char *private_key_path);

PULSAR_PUBLIC pulsar_consumer_crypto_failure_action pulsar_consumer_configuration_get_crypto_failure_action(
    pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_configuration_set_crypto_failure_action(
    pulsar_consumer_configuration_t *consumer_configuration,
    pulsar_consumer_crypto_failure_action cryptoFailureAction);

PULSAR_PUBLIC int pulsar_consumer_is_read_compacted(pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_set_read_compacted(pulsar_consumer_configuration_t *consumer_configuration,
                                                      int compacted);

PULSAR_PUBLIC int pulsar_consumer_get_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration);

PULSAR_PUBLIC void pulsar_consumer_set_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration, initial_position subscriptionInitialPosition);

PULSAR_PUBLIC void pulsar_consumer_configuration_set_property(pulsar_consumer_configuration_t *conf,
                                                              const char *name, const char *value);

// const CryptoKeyReaderPtr getCryptoKeyReader()
//
// const;
// ConsumerConfiguration&
// setCryptoKeyReader(CryptoKeyReaderPtr
// cryptoKeyReader);
//
// ConsumerCryptoFailureAction getCryptoFailureAction()
//
// const;
// ConsumerConfiguration&
// setCryptoFailureAction(ConsumerCryptoFailureAction
// action);

#ifdef __cplusplus
}
#endif
