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

/**
 * An interface that abstracts behavior of Pulsar's consumer.
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
    Message<byte[]> receive() throws PulsarClientException;

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
    CompletableFuture<Message<byte[]>> receiveAsync();

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
    Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException;

    /**
     * Acknowledge the consumption of a single message
     *
     * @param message
     *            The {@code Message} to be acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledge(Message<?> message) throws PulsarClientException;

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
    void acknowledgeCumulative(Message<?> message) throws PulsarClientException;

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
    CompletableFuture<Void> acknowledgeAsync(Message<?> message);

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
    CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message);

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
     * Get statistics for the consumer.
     *
     * <ul>
     * <li>numMsgsReceived : Number of messages received in the current interval
     * <li>numBytesReceived : Number of bytes received in the current interval
     * <li>numReceiveFailed : Number of messages failed to receive in the current interval
     * <li>numAcksSent : Number of acks sent in the current interval
     * <li>numAcksFailed : Number of acks failed to send in the current interval
     * <li>totalMsgsReceived : Total number of messages received
     * <li>totalBytesReceived : Total number of bytes received
     * <li>totalReceiveFailed : Total number of messages failed to receive
     * <li>totalAcksSent : Total number of acks sent
     * <li>totalAcksFailed : Total number of acks failed to sent
     * </ul>
     *
     * @return statistic for the consumer
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
     *
     * Please note that this does not simply mean that the consumer is caught up with the last message published by
     * producers, rather the topic needs to be explicitly "terminated".
     */
    boolean hasReachedEndOfTopic();

    /**
     * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    void redeliverUnacknowledgedMessages();

    /**
     * Reset the subscription associated with this consumer to a specific message id.
     * <p>
     *
     * The message id can either be a specific message or represent the first or last messages in the topic.
     * <p>
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
     * the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    void seek(MessageId messageId) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
     * the individual partitions.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     */
    void seek(long timestamp) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message id.
     * <p>
     *
     * The message id can either be a specific message or represent the first or last messages in the topic.
     * <p>
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
     * the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(MessageId messageId);

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
     * the individual partitions.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(long timestamp);

    /**
     * @return Whether the consumer is connected to the broker
     */
    boolean isConnected();

    /**
     * Get the name of consumer.
     * @return consumer name.
     */
    String getConsumerName();

    /**
     * Stop requesting new messages from the broker until {@link #resume()} is called. Note that this might cause
     * {@link #receive()} to block until {@link #resume()} is called and new messages are pushed by the broker.
     */
    void pause();

    /**
     * Resume requesting messages from the broker.
     */
    void resume();
}
