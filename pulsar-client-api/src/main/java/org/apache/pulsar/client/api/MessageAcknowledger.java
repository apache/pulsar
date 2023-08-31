/*
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
package org.apache.pulsar.client.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.transaction.Transaction;

/**
 * The interface to acknowledge one or more messages individually or cumulatively.
 * <p>
 * It contains two methods of various overloads:
 * - `acknowledge`: acknowledge individually
 * - `acknowledgeCumulative`: acknowledge cumulatively
 * Each of them has an associated asynchronous API that has the "Async" suffix in the name.
 * <p>
 * The 1st method parameter is
 * - {@link MessageId} or {@link Message} when acknowledging a single message
 * - {@link java.util.List<MessageId>} or {@link Messages} when acknowledging multiple messages
 * <p>
 * The 2nd method parameter is optional. Specify a non-null {@link Transaction} instance for transaction usages:
 * - After the transaction is committed, the message will be actually acknowledged (individually or cumulatively).
 * - After the transaction is aborted, the message will be redelivered.
 * @see Transaction#commit()
 * @see Transaction#abort()
 */
public interface MessageAcknowledger {

    /**
     * Acknowledge the consumption of a single message.
     *
     * @param messageId {@link MessageId} to be individual acknowledged
     *
     * @throws PulsarClientException.AlreadyClosedException}
     *             if the consumer was already closed
     * @throws PulsarClientException.NotAllowedException
     *             if `messageId` is not a {@link TopicMessageId} when multiple topics are subscribed
     */
    void acknowledge(MessageId messageId) throws PulsarClientException;

    default void acknowledge(Message<?> message) throws PulsarClientException {
        acknowledge(message.getMessageId());
    }

    /**
     * Acknowledge the consumption of a list of message.
     * @param messageIdList the list of message IDs.
     * @throws PulsarClientException.NotAllowedException
     *     if any message id in the list is not a {@link TopicMessageId} when multiple topics are subscribed
     */
    void acknowledge(List<MessageId> messageIdList) throws PulsarClientException;

    default void acknowledge(Messages<?> messages) throws PulsarClientException {
        for (Message<?> message : messages) {
            acknowledge(message.getMessageId());
        }
    }

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
     *
     * <p>This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
     * re-delivered to this consumer.
     *
     * <p>Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * <p>It's equivalent to calling asyncAcknowledgeCumulative(MessageId) and waiting for the callback to be triggered.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws PulsarClientException.NotAllowedException
     *             if `messageId` is not a {@link TopicMessageId} when multiple topics are subscribed
     */
    void acknowledgeCumulative(MessageId messageId) throws PulsarClientException;

    default void acknowledgeCumulative(Message<?> message) throws PulsarClientException {
        acknowledgeCumulative(message.getMessageId());
    }

    /**
     * The asynchronous version of {@link #acknowledge(MessageId)} with transaction support.
     */
    CompletableFuture<Void> acknowledgeAsync(MessageId messageId, Transaction txn);

    /**
     * The asynchronous version of {@link #acknowledge(MessageId)}.
     */
    default CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return acknowledgeAsync(messageId, null);
    }

    /**
     * The asynchronous version of {@link #acknowledge(List)} with transaction support.
     */
    CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList, Transaction txn);

    /**
     * The asynchronous version of {@link #acknowledge(List)}.
     */
    CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList);

    /**
     * The asynchronous version of {@link #acknowledge(Message)}.
     */
    CompletableFuture<Void> acknowledgeAsync(Message<?> message);

    /**
     * The asynchronous version of {@link #acknowledge(Messages)}.
     */
    CompletableFuture<Void> acknowledgeAsync(Messages<?> messages);

    /**
     * The asynchronous version of {@link #acknowledge(Messages)} with transaction support.
     */
    CompletableFuture<Void> acknowledgeAsync(Messages<?> messages, Transaction txn);

    /**
     * The asynchronous version of {@link #acknowledgeCumulative(MessageId)} with transaction support.
     *
     * @apiNote It's not allowed to cumulative ack with a transaction different from the previous one when the previous
     * transaction is not committed or aborted.
     * @apiNote It cannot be used for {@link SubscriptionType#Shared} subscription.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @param txn {@link Transaction} the transaction to cumulative ack
     */
    CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId,
                                                       Transaction txn);

    /**
     * The asynchronous version of {@link #acknowledgeCumulative(Message)}.
     */
    default CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
        return acknowledgeCumulativeAsync(message.getMessageId());
    }

    /**
     * The asynchronous version of {@link #acknowledgeCumulative(MessageId)}.
     */
    default CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        return acknowledgeCumulativeAsync(messageId, null);
    }
}
