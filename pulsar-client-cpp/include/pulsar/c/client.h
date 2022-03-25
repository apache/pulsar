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
#include <pulsar/c/client_configuration.h>
#include <pulsar/c/message.h>
#include <pulsar/c/message_id.h>
#include <pulsar/c/producer.h>
#include <pulsar/c/consumer.h>
#include <pulsar/c/reader.h>
#include <pulsar/c/consumer_configuration.h>
#include <pulsar/c/producer_configuration.h>
#include <pulsar/c/reader_configuration.h>
#include <pulsar/c/result.h>
#include <pulsar/c/string_list.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_client pulsar_client_t;
typedef struct _pulsar_producer pulsar_producer_t;
typedef struct _pulsar_string_list pulsar_string_list_t;

typedef struct _pulsar_client_configuration pulsar_client_configuration_t;
typedef struct _pulsar_producer_configuration pulsar_producer_configuration_t;

typedef void (*pulsar_create_producer_callback)(pulsar_result result, pulsar_producer_t *producer, void *ctx);

typedef void (*pulsar_subscribe_callback)(pulsar_result result, pulsar_consumer_t *consumer, void *ctx);
typedef void (*pulsar_reader_callback)(pulsar_result result, pulsar_reader_t *reader, void *ctx);
typedef void (*pulsar_get_partitions_callback)(pulsar_result result, pulsar_string_list_t *partitions,
                                               void *ctx);

typedef void (*pulsar_close_callback)(pulsar_result result, void *ctx);

/**
 * Create a Pulsar client object connecting to the specified cluster address and using the specified
 * configuration.
 *
 * @param serviceUrl the Pulsar endpoint to use (eg: pulsar://broker-example.com:6650)
 * @param clientConfiguration the client configuration to use
 */
PULSAR_PUBLIC pulsar_client_t *pulsar_client_create(const char *serviceUrl,
                                                    const pulsar_client_configuration_t *clientConfiguration);

/**
 * Create a producer with default configuration
 *
 * @see createProducer(const std::string&, const ProducerConfiguration&, Producer&)
 *
 * @param topic the topic where the new producer will publish
 * @param producer a non-const reference where the new producer will be copied
 * @return ResultOk if the producer has been successfully created
 * @return ResultError if there was an error
 */
PULSAR_PUBLIC pulsar_result pulsar_client_create_producer(pulsar_client_t *client, const char *topic,
                                                          const pulsar_producer_configuration_t *conf,
                                                          pulsar_producer_t **producer);

PULSAR_PUBLIC void pulsar_client_create_producer_async(pulsar_client_t *client, const char *topic,
                                                       const pulsar_producer_configuration_t *conf,
                                                       pulsar_create_producer_callback callback, void *ctx);

PULSAR_PUBLIC pulsar_result pulsar_client_subscribe(pulsar_client_t *client, const char *topic,
                                                    const char *subscriptionName,
                                                    const pulsar_consumer_configuration_t *conf,
                                                    pulsar_consumer_t **consumer);

PULSAR_PUBLIC void pulsar_client_subscribe_async(pulsar_client_t *client, const char *topic,
                                                 const char *subscriptionName,
                                                 const pulsar_consumer_configuration_t *conf,
                                                 pulsar_subscribe_callback callback, void *ctx);

/**
 * Create a consumer to multiple topics under the same namespace with default configuration
 *
 * @see subscribe(const std::vector<std::string>&, const std::string&, Consumer& consumer)
 *
 * @param topics a list of topic names to subscribe to
 * @param topicsCount the number of topics
 * @param subscriptionName the subscription name
 * @param consumer a non-const reference where the new consumer will be copied
 * @return ResultOk if the consumer has been successfully created
 * @return ResultError if there was an error
 */
PULSAR_PUBLIC pulsar_result pulsar_client_subscribe_multi_topics(pulsar_client_t *client, const char **topics,
                                                                 int topicsCount,
                                                                 const char *subscriptionName,
                                                                 const pulsar_consumer_configuration_t *conf,
                                                                 pulsar_consumer_t **consumer);

PULSAR_PUBLIC void pulsar_client_subscribe_multi_topics_async(pulsar_client_t *client, const char **topics,
                                                              int topicsCount, const char *subscriptionName,
                                                              const pulsar_consumer_configuration_t *conf,
                                                              pulsar_subscribe_callback callback, void *ctx);

/**
 * Create a consumer to multiple (which match given topicPattern) with default configuration
 *
 * @see subscribeWithRegex(const std::string&, const std::string&, Consumer& consumer)
 *
 * @param topicPattern topic regex topics should match to subscribe to
 * @param subscriptionName the subscription name
 * @param consumer a non-const reference where the new consumer will be copied
 * @return ResultOk if the consumer has been successfully created
 * @return ResultError if there was an error
 */
PULSAR_PUBLIC pulsar_result pulsar_client_subscribe_pattern(pulsar_client_t *client, const char *topicPattern,
                                                            const char *subscriptionName,
                                                            const pulsar_consumer_configuration_t *conf,
                                                            pulsar_consumer_t **consumer);

PULSAR_PUBLIC void pulsar_client_subscribe_pattern_async(pulsar_client_t *client, const char *topicPattern,
                                                         const char *subscriptionName,
                                                         const pulsar_consumer_configuration_t *conf,
                                                         pulsar_subscribe_callback callback, void *ctx);

/**
 * Create a topic reader with given {@code ReaderConfiguration} for reading messages from the specified
 * topic.
 * <p>
 * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without
 * using a
 * subscription. Reader can only work on non-partitioned topics.
 * <p>
 * The initial reader positioning is done by specifying a message id. The options are:
 * <ul>
 * <li><code>MessageId.earliest</code> : Start reading from the earliest message available in the topic
 * <li><code>MessageId.latest</code> : Start reading from the end topic, only getting messages published
 * after the
 * reader was created
 * <li><code>MessageId</code> : When passing a particular message id, the reader will position itself on
 * that
 * specific position. The first message to be read will be the message next to the specified messageId.
 * </ul>
 *
 * @param topic
 *            The name of the topic where to read
 * @param startMessageId
 *            The message id where the reader will position itself. The first message returned will be the
 * one after
 *            the specified startMessageId
 * @param conf
 *            The {@code ReaderConfiguration} object
 * @return The {@code Reader} object
 */
PULSAR_PUBLIC pulsar_result pulsar_client_create_reader(pulsar_client_t *client, const char *topic,
                                                        const pulsar_message_id_t *startMessageId,
                                                        pulsar_reader_configuration_t *conf,
                                                        pulsar_reader_t **reader);

PULSAR_PUBLIC void pulsar_client_create_reader_async(pulsar_client_t *client, const char *topic,
                                                     const pulsar_message_id_t *startMessageId,
                                                     pulsar_reader_configuration_t *conf,
                                                     pulsar_reader_callback callback, void *ctx);

PULSAR_PUBLIC pulsar_result pulsar_client_get_topic_partitions(pulsar_client_t *client, const char *topic,
                                                               pulsar_string_list_t **partitions);

PULSAR_PUBLIC void pulsar_client_get_topic_partitions_async(pulsar_client_t *client, const char *topic,
                                                            pulsar_get_partitions_callback callback,
                                                            void *ctx);

PULSAR_PUBLIC pulsar_result pulsar_client_close(pulsar_client_t *client);

PULSAR_PUBLIC void pulsar_client_close_async(pulsar_client_t *client, pulsar_close_callback callback,
                                             void *ctx);

PULSAR_PUBLIC void pulsar_client_free(pulsar_client_t *client);

#ifdef __cplusplus
}
#endif
