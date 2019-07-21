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

#include <pulsar/defines.h>
#include <pulsar/c/result.h>
#include <pulsar/c/message.h>

#include <stdint.h>

typedef struct _pulsar_producer pulsar_producer_t;

typedef void (*pulsar_send_callback)(pulsar_result, pulsar_message_t *msg, void *ctx);
typedef void (*pulsar_close_callback)(pulsar_result, void *ctx);
typedef void (*pulsar_flush_callback)(pulsar_result, void *ctx);

/**
 * @return the topic to which producer is publishing to
 */
PULSAR_PUBLIC const char *pulsar_producer_get_topic(pulsar_producer_t *producer);

/**
 * @return the producer name which could have been assigned by the system or specified by the client
 */
PULSAR_PUBLIC const char *pulsar_producer_get_producer_name(pulsar_producer_t *producer);

/**
 * Publish a message on the topic associated with this Producer.
 *
 * This method will block until the message will be accepted and persisted
 * by the broker. In case of errors, the client library will try to
 * automatically recover and use a different broker.
 *
 * If it wasn't possible to successfully publish the message within the sendTimeout,
 * an error will be returned.
 *
 * This method is equivalent to asyncSend() and wait until the callback is triggered.
 *
 * @param msg message to publish
 * @return ResultOk if the message was published successfully
 * @return ResultWriteError if it wasn't possible to publish the message
 */
PULSAR_PUBLIC pulsar_result pulsar_producer_send(pulsar_producer_t *producer, pulsar_message_t *msg);

/**
 * Asynchronously publish a message on the topic associated with this Producer.
 *
 * This method will initiate the publish operation and return immediately. The
 * provided callback will be triggered when the message has been be accepted and persisted
 * by the broker. In case of errors, the client library will try to
 * automatically recover and use a different broker.
 *
 * If it wasn't possible to successfully publish the message within the sendTimeout, the
 * callback will be triggered with a Result::WriteError code.
 *
 * @param msg message to publish
 * @param callback the callback to get notification of the completion
 */
PULSAR_PUBLIC void pulsar_producer_send_async(pulsar_producer_t *producer, pulsar_message_t *msg,
                                              pulsar_send_callback callback, void *ctx);

/**
 * Get the last sequence id that was published by this producer.
 *
 * This represent either the automatically assigned or custom sequence id (set on the MessageBuilder) that
 * was published and acknowledged by the broker.
 *
 * After recreating a producer with the same producer name, this will return the last message that was
 * published in
 * the previous producer session, or -1 if there no message was ever published.
 *
 * @return the last sequence id published by this producer
 */
PULSAR_PUBLIC int64_t pulsar_producer_get_last_sequence_id(pulsar_producer_t *producer);

/**
 * Close the producer and release resources allocated.
 *
 * No more writes will be accepted from this producer. Waits until
 * all pending write requests are persisted. In case of errors,
 * pending writes will not be retried.
 *
 * @return an error code to indicate the success or failure
 */
PULSAR_PUBLIC pulsar_result pulsar_producer_close(pulsar_producer_t *producer);

/**
 * Close the producer and release resources allocated.
 *
 * No more writes will be accepted from this producer. The provided callback will be
 * triggered when all pending write requests are persisted. In case of errors,
 * pending writes will not be retried.
 */
PULSAR_PUBLIC void pulsar_producer_close_async(pulsar_producer_t *producer, pulsar_close_callback callback,
                                               void *ctx);

// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
PULSAR_PUBLIC pulsar_result pulsar_producer_flush(pulsar_producer_t *producer);

PULSAR_PUBLIC void pulsar_producer_flush_async(pulsar_producer_t *producer, pulsar_flush_callback callback,
                                               void *ctx);

PULSAR_PUBLIC void pulsar_producer_free(pulsar_producer_t *producer);

#ifdef __cplusplus
}
#endif