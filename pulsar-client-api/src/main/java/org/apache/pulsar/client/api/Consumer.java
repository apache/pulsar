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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * An interface that abstracts behavior of Pulsar's consumer.
 *
 * <p>All the operations on the consumer instance are thread safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Consumer<T> extends Closeable, MessageAcknowledger {

    /**
     * Get a topic for the consumer.
     *
     * @return topic for the consumer
     */
    String getTopic();

    /**
     * Get a subscription for the consumer.
     *
     * @return subscription for the consumer
     */
    String getSubscription();

    /**
     * Unsubscribe the consumer.
     *
     * <p>This call blocks until the consumer is unsubscribed.
     *
     * <p>Unsubscribing will the subscription to be deleted and all the
     * data retained can potentially be deleted as well.
     *
     * <p>The operation will fail when performed on a shared subscription
     * where multiple consumers are currently connected.
     *
     * @throws PulsarClientException if the operation fails
     */
    void unsubscribe() throws PulsarClientException;

    /**
     * Asynchronously unsubscribe the consumer.
     *
     * @see Consumer#unsubscribe()
     * @return {@link CompletableFuture} to track the operation
     */
    CompletableFuture<Void> unsubscribeAsync();


    /**
     * Unsubscribe the consumer.
     *
     * <p>This call blocks until the consumer is unsubscribed.
     *
     * <p>Unsubscribing will the subscription to be deleted and all the
     * data retained can potentially be deleted as well.
     *
     * <p>The operation will fail when performed on a shared subscription
     * where multiple consumers are currently connected.
     *
     * @param force forcefully unsubscribe by disconnecting connected consumers.
     * @throws PulsarClientException if the operation fails
     */
    void unsubscribe(boolean force) throws PulsarClientException;

    /**
     * Asynchronously unsubscribe the consumer.
     *
     * @see Consumer#unsubscribe()
     * @param force forcefully unsubscribe by disconnecting connected consumers.
     * @return {@link CompletableFuture} to track the operation
     */
    CompletableFuture<Void> unsubscribeAsync(boolean force);
    /**
     * Receives a single message in blocking mode.
     *
     * <p>This method blocks until a message is available or the consumer is closed.
     *
     * <p>Behavior when interrupted:
     * <ul>
     *   <li>If the thread is interrupted while waiting: returns null and resets the interrupted flag</li>
     *   <li>If the consumer is closed while waiting: throws {@link PulsarClientException} with the cause
     *       {@code InterruptedException("Queue is terminated")}</li>
     * </ul>
     *
     * @return the received message, or null if the thread was interrupted
     * @throws PulsarClientException if the consumer is closed while waiting for a message.
     *         The exception will contain an {@link InterruptedException} with the message
     *         "Queue is terminated" as its cause.
     * @throws PulsarClientException.AlreadyClosedException if the consumer was already closed
     *         before this method was called
     * @throws PulsarClientException.InvalidConfigurationException if a message listener
     *         was defined in the configuration
     */
    Message<T> receive() throws PulsarClientException;

    /**
     * Receive a single message
     *
     * <p>Retrieves a message when it will be available and completes {@link CompletableFuture} with received message.
     *
     * <p>{@code receiveAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete
     * with received message. Else it creates <i> backlog of receive requests </i> in the application.
     *
     * <p>The returned future can be cancelled before completion by calling {@code .cancel(false)}
     * ({@link CompletableFuture#cancel(boolean)}) to remove it from the the backlog of receive requests. Another
     * choice for ensuring a proper clean up of the returned future is to use the CompletableFuture.orTimeout method
     * which is available on JDK9+. That would remove it from the backlog of receive requests if receiving exceeds
     * the timeout.
     *
     * @return {@link CompletableFuture}<{@link Message}> will be completed when message is available
     */
    CompletableFuture<Message<T>> receiveAsync();

    /**
     * Receive a single message.
     *
     * <p>Retrieves a message, waiting up to the specified wait time if necessary.
     * <p>If consumer closes during wait: returns null immediately.
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
    Message<T> receive(int timeout, TimeUnit unit) throws PulsarClientException;

    /**
     * Batch receiving messages.
     *
     * <p>This calls blocks until has enough messages or wait timeout, more details to see {@link BatchReceivePolicy}.
     *
     * @return messages
     * @since 2.4.1
     * @throws PulsarClientException
     */
    Messages<T> batchReceive() throws PulsarClientException;

    /**
     * Batch receiving messages.
     * <p>
     * Retrieves messages when has enough messages or wait timeout and
     * completes {@link CompletableFuture} with received messages.
     * </p>
     * <p>
     * {@code batchReceiveAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete
     * with received messages. Else it creates <i> backlog of receive requests </i> in the application.
     * </p>
     *
     * <p>The returned future can be cancelled before completion by calling {@code .cancel(false)}
     * ({@link CompletableFuture#cancel(boolean)}) to remove it from the the backlog of receive requests. Another
     * choice for ensuring a proper clean up of the returned future is to use the CompletableFuture.orTimeout method
     * which is available on JDK9+. That would remove it from the backlog of receive requests if receiving exceeds
     * the timeout.
     *
     *
     * @return messages
     * @since 2.4.1
     * @throws PulsarClientException
     */
    CompletableFuture<Messages<T>> batchReceiveAsync();

    /**
     * Acknowledge the failure to process a single message.
     *
     * <p>When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Message&lt;String&gt; msg = consumer.receive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msg);
     *     }
     * }
     * </code></pre>
     *
     * @param message
     *            The {@code Message} to be acknowledged
     */
    void negativeAcknowledge(Message<?> message);

    /**
     * Acknowledge the failure to process a single message.
     *
     * <p>When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>This variation allows to pass a {@link MessageId} rather than a {@link Message}
     * object, in order to avoid keeping the payload in memory for extended amount
     * of time
     *
     * @see #negativeAcknowledge(Message)
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     */
    void negativeAcknowledge(MessageId messageId);

    /**
     * Acknowledge the failure to process {@link Messages}.
     *
     * <p>When messages is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Messages&lt;String&gt; msgs = consumer.batchReceive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msgs);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msgs);
     *     }
     * }
     * </code></pre>
     *
     * @param messages
     *            The {@code Message} to be acknowledged
     */
    void negativeAcknowledge(Messages<?> messages);

    /**
     * reconsumeLater the consumption of {@link Messages}.
     *
     *<p>When a message is "reconsumeLater" it will be marked for redelivery after
     * some custom delay.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Message&lt;String&gt; msg = consumer.receive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.reconsumeLater(msg, 1000, TimeUnit.MILLISECONDS);
     *     }
     * }
     * </code></pre>
     *
     * @param message
     *            the {@code Message} to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @throws PulsarClientException.AlreadyClosedException
     *              if the consumer was already closed
     */
    void reconsumeLater(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException;

    /**
     * reconsumeLater the consumption of {@link Messages}.
     *
     *<p>When a message is "reconsumeLater" it will be marked for redelivery after
     * some custom delay.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Message&lt;String&gt; msg = consumer.receive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.reconsumeLater(msg, 1000, TimeUnit.MILLISECONDS);
     *     }
     * }
     * </code></pre>
     *
     * @param message
     *            the {@code Message} to be reconsumeLater
     * @param customProperties
     *            the custom properties to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @throws PulsarClientException.AlreadyClosedException
     *              if the consumer was already closed
     */
    void reconsumeLater(Message<?> message,
                        Map<String, String> customProperties,
                        long delayTime, TimeUnit unit) throws PulsarClientException;

    /**
     * reconsumeLater the consumption of {@link Messages}.
     *
     * @param messages
     *            the {@code messages} to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @throws PulsarClientException.AlreadyClosedException
     *              if the consumer was already closed
     */
    void reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) throws PulsarClientException;

    /**
     * reconsumeLater the reception of all the messages in the stream up to (and including) the provided message.
     *
     * @param message
     *            The {@code message} to be cumulatively reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException;


    /**
     * Asynchronously reconsumeLater the consumption of a single message.
     *
     * @param message
     *            The {@code Message} to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> reconsumeLaterAsync(Message<?> message, long delayTime, TimeUnit unit);

    /**
     * Asynchronously reconsumeLater the consumption of a single message.
     *
     * @param message
     *            The {@code Message} to be reconsumeLater
     * @param customProperties
     *            The custom properties to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> reconsumeLaterAsync(Message<?> message,
                                                Map<String, String> customProperties,
                                                long delayTime, TimeUnit unit);

    /**
     * Asynchronously reconsumeLater the consumption of {@link Messages}.
     *
     * @param messages
     *            The {@link Messages} to be reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, long delayTime, TimeUnit unit);

    /**
     * Asynchronously ReconsumeLater the reception of all the messages in the stream up to (and including) the provided
     * message.
     *
     * <p>Cumulative reconsumeLater cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param message
     *            The {@code message} to be cumulatively reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message, long delayTime, TimeUnit unit);

    /**
     * Asynchronously ReconsumeLater the reception of all the messages in the stream up to (and including) the provided
     * message.
     *
     * <p>Cumulative reconsumeLater cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param message
     *            The {@code message} to be cumulatively reconsumeLater
     * @param customProperties
     *            The custom properties to be cumulatively reconsumeLater
     * @param delayTime
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message,
                                                          Map<String, String> customProperties,
                                                          long delayTime, TimeUnit unit);

    /**
     * Get statistics for the consumer.
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
     * Asynchronously close the consumer and stop the broker to push more messages.
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
     *
     * <p>Please note that this does not simply mean that the consumer is caught up with the last message published by
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
     * If there is already a seek operation in progress, the method will log a warning and
     * return a future completed exceptionally.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     * <p>
     * This effectively resets the acknowledgement state of the subscription: all messages up to and
     * <b>including</b> <code>messageId</code> will be marked as acknowledged and the rest unacknowledged.
     *
     * <p>Note: For multi-topics consumer, if `messageId` is a {@link TopicMessageId}, the seek operation will happen
     * on the owner topic of the message, which is returned by {@link TopicMessageId#getOwnerTopic()}. Otherwise, you
     * can only seek to the earliest or latest message for all topics subscribed.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    void seek(MessageId messageId) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     * <p>
     * If there is already a seek operation in progress, the method will log a warning and
     * return a future completed exceptionally.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     *            The timestamp format should be Unix time in milliseconds.
     */
    void seek(long timestamp) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message ID or message publish time.
     * <p>
     * If there is already a seek operation in progress, the method will log a warning and
     * return a future completed exceptionally.
     *
     * <p>
     * The Function input is topic+partition. It returns only timestamp or MessageId.
     * <p>
     * The return value is the seek position/timestamp of the current partition.
     * Exception is thrown if other object types are returned.
     * <p>
     * If returns null, the current partition will not do any processing.
     * Exception in a partition may affect other partitions.
     * @param function
     * @throws PulsarClientException
     */
    void seek(Function<String, Object> function) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message ID
     * or message publish time asynchronously.
     * <p>
     * The Function input is topic+partition. It returns only timestamp or MessageId.
     * <p>
     * The return value is the seek position/timestamp of the current partition.
     * Exception is thrown if other object types are returned.
     * <p>
     * If returns null, the current partition will not do any processing.
     * Exception in a partition may affect other partitions.
     * @param function
     * @return
     */
    CompletableFuture<Void> seekAsync(Function<String, Object> function);

    /**
     * The asynchronous version of {@link Consumer#seek(MessageId)}.
     * <p>
     * If there is already a seek operation in progress, the method will log a warning and
     * return a future completed exceptionally.
     */
    CompletableFuture<Void> seekAsync(MessageId messageId);

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     * <p>
     * If there is already a seek operation in progress, the method will log a warning and
     * return a future completed exceptionally.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     *            The timestamp format should be Unix time in milliseconds.
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(long timestamp);

    /**
     * Get the last message id available for consume.
     *
     * @return the last message id.
     * @apiNote If the consumer is a multi-topics consumer, the returned value cannot be used anywhere.
     * @deprecated Use {@link Consumer#getLastMessageIds()} instead.
     */
    @Deprecated
    MessageId getLastMessageId() throws PulsarClientException;

    /**
     * Get the last message id available for consume.
     *
     * @return a future that can be used to track the completion of the operation.
     * @deprecated Use {@link Consumer#getLastMessageIdsAsync()}} instead.
     */
    @Deprecated
    CompletableFuture<MessageId> getLastMessageIdAsync();

    /**
     * Get all the last message id of the topics the consumer subscribed.
     *
     * @return the list of TopicMessageId instances of all the topics that the consumer subscribed
     * @throws PulsarClientException if failed to get last message id.
     * @apiNote It's guaranteed that the owner topic of each TopicMessageId in the returned list is different from owner
     *   topics of other TopicMessageId instances
     */
    List<TopicMessageId> getLastMessageIds() throws PulsarClientException;

    /**
     * The asynchronous version of {@link Consumer#getLastMessageIds()}.
     */
    CompletableFuture<List<TopicMessageId>> getLastMessageIdsAsync();

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

    /**
     * @return The last disconnected timestamp of the consumer
     */
    long getLastDisconnectedTimestamp();
}
