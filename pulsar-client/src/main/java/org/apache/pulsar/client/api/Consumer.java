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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.ConsumerStats;

/**
 * An interface that abstracts behavior of Pulsar's consumer.
 *
 *
 */
public interface Consumer extends Closeable {

    /**
     * Get a topic for the consumer
     *
     * @return topic for the consumer
     */
    String getTopic();

    /**
     * Get a subscription for the consumer
     *
     * @return subscription for the consumer
     */
    String getSubscription();

    /**
     * Unsubscribe the consumer
     * <p>
     * This call blocks until the consumer is unsubscribed.
     *
     * @throws PulsarClientException
     */
    void unsubscribe() throws PulsarClientException;

    /**
     * Asynchronously unsubscribe the consumer
     *
     * @return {@link CompletableFuture} for this operation
     */
    CompletableFuture<Void> unsubscribeAsync();

    /**
     * Receives a single message.
     * <p>
     * This calls blocks until a message is available.
     *
     * @return the received message
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws PulsarClientException.InvalidConfigurationException
     *             if a message listener was defined in the configuration
     */
    Message receive() throws PulsarClientException;

    /**
     * Receive a single message
     * <p>
     * Retrieves a message when it will be available and completes {@link CompletableFuture} with received message.
     * </p>
     * <p>
     * {@code receiveAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete with
     * received message. Else it creates <i> backlog of receive requests </i> in the application.
     * </p>
     *
     * @return {@link CompletableFuture}<{@link Message}> will be completed when message is available
     */
    CompletableFuture<Message> receiveAsync();

    /**
     * Receive a single message
     * <p>
     * Retrieves a message, waiting up to the specified wait time if necessary.
     *
     * @param timeout
     *            0 or less means immediate rather than infinite
     * @param unit
     * @return the received {@link Message} or null if no message available before timeout
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws PulsarClientException.InvalidConfigurationException
     *             if a message listener was defined in the configuration
     */
    Message receive(int timeout, TimeUnit unit) throws PulsarClientException;

    /**
     * Acknowledge the consumption of a single message
     *
     * @param message
     *            The {@code Message} to be acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledge(Message message) throws PulsarClientException;

    /**
     * Acknowledge the consumption of a single message, identified by its MessageId
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledge(MessageId messageId) throws PulsarClientException;

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
     *
     * This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
     * re-delivered to this consumer.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * It's equivalent to calling asyncAcknowledgeCumulative(Message) and waiting for the callback to be triggered.
     *
     * @param message
     *            The {@code Message} to be cumulatively acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledgeCumulative(Message message) throws PulsarClientException;

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
     *
     * This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
     * re-delivered to this consumer.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * It's equivalent to calling asyncAcknowledgeCumulative(MessageId) and waiting for the callback to be triggered.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledgeCumulative(MessageId messageId) throws PulsarClientException;

    /**
     * Asynchronously acknowledge the consumption of a single message
     *
     * @param message
     *            The {@code Message} to be acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeAsync(Message message);

    /**
     * Asynchronously acknowledge the consumption of a single message
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeAsync(MessageId messageId);

    /**
     * Asynchronously Acknowledge the reception of all the messages in the stream up to (and including) the provided
     * message.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param message
     *            The {@code Message} to be cumulatively acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeCumulativeAsync(Message message);

    /**
     * Asynchronously Acknowledge the reception of all the messages in the stream up to (and including) the provided
     * message.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId);

    /**
     * Get statistics for the consumer
     *
     * numMsgsReceived : Number of messages received in the current interval numBytesReceived : Number of bytes received
     * in the current interval numReceiveFailed : Number of messages failed to receive in the current interval
     * numAcksSent : Number of acks sent in the current interval numAcksFailed : Number of acks failed to send in the
     * current interval totalMsgsReceived : Total number of messages received totalBytesReceived : Total number of bytes
     * received totalReceiveFailed : Total number of messages failed to receive totalAcksSent : Total number of acks
     * sent totalAcksFailed : Total number of acks failed to sent
     *
     * @return statistic for the consumer or null if ConsumerStats is disabled.
     */
    ConsumerStats getStats();

    /**
     * Close the consumer and stop the broker to push more messages.
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Asynchronously close the consumer and stop the broker to push more messages
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
     */
    boolean hasReachedEndOfTopic();

    /**
     * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    void redeliverUnacknowledgedMessages();
}
