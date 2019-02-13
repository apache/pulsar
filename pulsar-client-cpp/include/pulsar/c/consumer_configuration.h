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

#include "consumer.h"

#ifdef __cplusplus
extern "C" {
#endif

#pragma GCC visibility push(default)

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
    pulsar_ConsumerFailover
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

/// Callback definition for MessageListener
typedef void (*pulsar_message_listener)(pulsar_consumer_t *consumer, pulsar_message_t *msg, void *ctx);

pulsar_consumer_configuration_t *pulsar_consumer_configuration_create();

void pulsar_consumer_configuration_free(pulsar_consumer_configuration_t *consumer_configuration);

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
void pulsar_consumer_configuration_set_consumer_type(pulsar_consumer_configuration_t *consumer_configuration,
                                                     pulsar_consumer_type consumerType);

pulsar_consumer_type pulsar_consumer_configuration_get_consumer_type(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * A message listener enables your application to configure how to process
 * and acknowledge messages delivered. A listener will be called in order
 * for every message received.
 */
void pulsar_consumer_configuration_set_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_message_listener messageListener,
    void *ctx);

int pulsar_consumer_configuration_has_message_listener(
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
void pulsar_consumer_configuration_set_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration, int size);

int pulsar_consumer_configuration_get_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set the max total receiver queue size across partitons.
 * <p>
 * This setting will be used to reduce the receiver queue size for individual partitions
 * {@link #setReceiverQueueSize(int)} if the total exceeds this value (default: 50000).
 *
 * @param maxTotalReceiverQueueSizeAcrossPartitions
 */
void pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration, int maxTotalReceiverQueueSizeAcrossPartitions);

/**
 * @return the configured max total receiver queue size across partitions
 */
int pulsar_consumer_get_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration);

void pulsar_consumer_set_consumer_name(pulsar_consumer_configuration_t *consumer_configuration,
                                       const char *consumerName);

const char *pulsar_consumer_get_consumer_name(pulsar_consumer_configuration_t *consumer_configuration);

/**
 * Set the timeout in milliseconds for unacknowledged messages, the timeout needs to be greater than
 * 10 seconds. An Exception is thrown if the given value is less than 10000 (10 seconds).
 * If a successful acknowledgement is not sent within the timeout all the unacknowledged messages are
 * redelivered.
 * @param timeout in milliseconds
 */
void pulsar_consumer_set_unacked_messages_timeout_ms(pulsar_consumer_configuration_t *consumer_configuration,
                                                     const uint64_t milliSeconds);

/**
 * @return the configured timeout in milliseconds for unacked messages.
 */
long pulsar_consumer_get_unacked_messages_timeout_ms(pulsar_consumer_configuration_t *consumer_configuration);

int pulsar_consumer_is_encryption_enabled(pulsar_consumer_configuration_t *consumer_configuration);

int pulsar_consumer_is_read_compacted(pulsar_consumer_configuration_t *consumer_configuration);

void pulsar_consumer_set_read_compacted(pulsar_consumer_configuration_t *consumer_configuration,
                                        int compacted);

int pulsar_consumer_get_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration);

void pulsar_consumer_set_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration, initial_position subscriptionInitialPosition);

void pulsar_consumer_configuration_set_property(pulsar_consumer_configuration_t *conf, const char *name,
                                                const char *value);

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

#pragma GCC visibility pop

#ifdef __cplusplus
}
#endif
