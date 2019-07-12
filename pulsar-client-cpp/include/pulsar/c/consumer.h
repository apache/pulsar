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

#ifdef __cplusplus
extern "C" {
#endif

#include <pulsar/c/result.h>
#include <pulsar/c/message.h>

#include <stdint.h>

typedef struct _pulsar_consumer pulsar_consumer_t;

typedef void (*pulsar_result_callback)(pulsar_result, void *);

/**
 * @return the topic this consumer is subscribed to
 */
PULSAR_PUBLIC const char *pulsar_consumer_get_topic(pulsar_consumer_t *consumer);

/**
 * @return the consumer name
 */
PULSAR_PUBLIC const char *pulsar_consumer_get_subscription_name(pulsar_consumer_t *consumer);

/**
 * Unsubscribe the current consumer from the topic.
 *
 * This method will block until the operation is completed. Once the consumer is
 * unsubscribed, no more messages will be received and subsequent new messages
 * will not be retained for this consumer.
 *
 * This consumer object cannot be reused.
 *
 * @see asyncUnsubscribe
 * @return Result::ResultOk if the unsubscribe operation completed successfully
 * @return Result::ResultError if the unsubscribe operation failed
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_unsubscribe(pulsar_consumer_t *consumer);

/**
 * Asynchronously unsubscribe the current consumer from the topic.
 *
 * This method will block until the operation is completed. Once the consumer is
 * unsubscribed, no more messages will be received and subsequent new messages
 * will not be retained for this consumer.
 *
 * This consumer object cannot be reused.
 *
 * @param callback the callback to get notified when the operation is complete
 */
PULSAR_PUBLIC void pulsar_consumer_unsubscribe_async(pulsar_consumer_t *consumer,
                                                     pulsar_result_callback callback, void *ctx);

/**
 * Receive a single message.
 *
 * If a message is not immediately available, this method will block until a new
 * message is available.
 *
 * @param msg a non-const reference where the received message will be copied
 * @return ResultOk when a message is received
 * @return ResultInvalidConfiguration if a message listener had been set in the configuration
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_receive(pulsar_consumer_t *consumer, pulsar_message_t **msg);

/**
 *
 * @param msg a non-const reference where the received message will be copied
 * @param timeoutMs the receive timeout in milliseconds
 * @return ResultOk if a message was received
 * @return ResultTimeout if the receive timeout was triggered
 * @return ResultInvalidConfiguration if a message listener had been set in the configuration
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_receive_with_timeout(pulsar_consumer_t *consumer,
                                                                 pulsar_message_t **msg, int timeoutMs);

/**
 * Acknowledge the reception of a single message.
 *
 * This method will block until an acknowledgement is sent to the broker. After
 * that, the message will not be re-delivered to this consumer.
 *
 * @see asyncAcknowledge
 * @param message the message to acknowledge
 * @return ResultOk if the message was successfully acknowledged
 * @return ResultError if there was a failure
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_acknowledge(pulsar_consumer_t *consumer,
                                                        pulsar_message_t *message);

PULSAR_PUBLIC pulsar_result pulsar_consumer_acknowledge_id(pulsar_consumer_t *consumer,
                                                           pulsar_message_id_t *messageId);

/**
 * Asynchronously acknowledge the reception of a single message.
 *
 * This method will initiate the operation and return immediately. The provided callback
 * will be triggered when the operation is complete.
 *
 * @param message the message to acknowledge
 * @param callback callback that will be triggered when the message has been acknowledged
 */
PULSAR_PUBLIC void pulsar_consumer_acknowledge_async(pulsar_consumer_t *consumer, pulsar_message_t *message,
                                                     pulsar_result_callback callback, void *ctx);

PULSAR_PUBLIC void pulsar_consumer_acknowledge_async_id(pulsar_consumer_t *consumer,
                                                        pulsar_message_id_t *messageId,
                                                        pulsar_result_callback callback, void *ctx);

/**
 * Acknowledge the reception of all the messages in the stream up to (and including)
 * the provided message.
 *
 * This method will block until an acknowledgement is sent to the broker. After
 * that, the messages will not be re-delivered to this consumer.
 *
 * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
 *
 * It's equivalent to calling asyncAcknowledgeCumulative(const Message&, ResultCallback) and
 * waiting for the callback to be triggered.
 *
 * @param message the last message in the stream to acknowledge
 * @return ResultOk if the message was successfully acknowledged. All previously delivered messages for
 * this topic are also acknowledged.
 * @return ResultError if there was a failure
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_acknowledge_cumulative(pulsar_consumer_t *consumer,
                                                                   pulsar_message_t *message);

PULSAR_PUBLIC pulsar_result pulsar_consumer_acknowledge_cumulative_id(pulsar_consumer_t *consumer,
                                                                      pulsar_message_id_t *messageId);

/**
 * Asynchronously acknowledge the reception of all the messages in the stream up to (and
 * including) the provided message.
 *
 * This method will initiate the operation and return immediately. The provided callback
 * will be triggered when the operation is complete.
 *
 * @param message the message to acknowledge
 * @param callback callback that will be triggered when the message has been acknowledged
 */
PULSAR_PUBLIC void pulsar_consumer_acknowledge_cumulative_async(pulsar_consumer_t *consumer,
                                                                pulsar_message_t *message,
                                                                pulsar_result_callback callback, void *ctx);

PULSAR_PUBLIC void pulsar_consumer_acknowledge_cumulative_async_id(pulsar_consumer_t *consumer,
                                                                   pulsar_message_id_t *messageId,
                                                                   pulsar_result_callback callback,
                                                                   void *ctx);

/**
 * Acknowledge the failure to process a single message.
 * <p>
 * When a message is "negatively acked" it will be marked for redelivery after
 * some fixed delay. The delay is configurable when constructing the consumer
 * with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
 * <p>
 * This call is not blocking.
 *
 * @param message
 *            The {@code Message} to be acknowledged
 */
PULSAR_PUBLIC void pulsar_consumer_negative_acknowledge(pulsar_consumer_t *consumer,
                                                        pulsar_message_t *message);

/**
 * Acknowledge the failure to process a single message through its message id
 * <p>
 * When a message is "negatively acked" it will be marked for redelivery after
 * some fixed delay. The delay is configurable when constructing the consumer
 * with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
 * <p>
 * This call is not blocking.
 *
 * @param message
 *            The message id to be acknowledged
 */
PULSAR_PUBLIC void pulsar_consumer_negative_acknowledge_id(pulsar_consumer_t *consumer,
                                                           pulsar_message_id_t *messageId);

PULSAR_PUBLIC pulsar_result pulsar_consumer_close(pulsar_consumer_t *consumer);

PULSAR_PUBLIC void pulsar_consumer_close_async(pulsar_consumer_t *consumer, pulsar_result_callback callback,
                                               void *ctx);

PULSAR_PUBLIC void pulsar_consumer_free(pulsar_consumer_t *consumer);

/*
 * Pause receiving messages via the messageListener, till resumeMessageListener() is called.
 */
PULSAR_PUBLIC pulsar_result pulsar_consumer_pause_message_listener(pulsar_consumer_t *consumer);

/*
 * Resume receiving the messages via the messageListener.
 * Asynchronously receive all the messages enqueued from time pauseMessageListener() was called.
 */
PULSAR_PUBLIC pulsar_result resume_message_listener(pulsar_consumer_t *consumer);

/**
 * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is
 * not
 * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed
 * across all
 * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the
 * connection
 * breaks, the messages are redelivered after reconnect.
 */
PULSAR_PUBLIC void pulsar_consumer_redeliver_unacknowledged_messages(pulsar_consumer_t *consumer);

PULSAR_PUBLIC void pulsar_consumer_seek_async(pulsar_consumer_t *consumer, pulsar_message_id_t *messageId,
                                              pulsar_result_callback callback, void *ctx);

PULSAR_PUBLIC pulsar_result pulsar_consumer_seek(pulsar_consumer_t *consumer, pulsar_message_id_t *messageId);

#ifdef __cplusplus
}
#endif
