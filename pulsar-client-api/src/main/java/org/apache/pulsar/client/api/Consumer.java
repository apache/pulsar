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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * An interface that abstracts behavior of Pulsar's consumer.
 *
 * <p>All the operations on the consumer instance are thread safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Consumer<T> extends Closeable {

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
     * Receives a single message.
     *
     * <p>This calls blocks until a message is available.
     *
     * @return the received message
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws PulsarClientException.InvalidConfigurationException
     *             if a message listener was defined in the configuration
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
     * Acknowledge the consumption of a single message.
     *
     * @param message
     *            The {@code Message} to be acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledge(Message<?> message) throws PulsarClientException;

    /**
     * Acknowledge the consumption of a single message, identified by its {@link MessageId}.
     *
     * @param messageId
     *            The {@link MessageId} to be acknowledged
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     */
    void acknowledge(MessageId messageId) throws PulsarClientException;

    /**
     * Acknowledge the consumption of {@link Messages}.
     *
     * @param messages messages
     * @throws PulsarClientException.AlreadyClosedException
     *              if the consumer was already closed
     */
    void acknowledge(Messages<?> messages) throws PulsarClientException;

    /**
     * Acknowledge the consumption of a list of message.
     * @param messageIdList
     * @throws PulsarClientException
     */
    void acknowledge(List<MessageId> messageIdList) throws PulsarClientException;

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
     * Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
     *
     * <p>This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
     * re-delivered to this consumer.
     *
     * <p>Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * <p>It's equivalent to calling asyncAcknowledgeCumulative(Message) and waiting for the callback to be triggered.
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
     */
    void acknowledgeCumulative(MessageId messageId) throws PulsarClientException;

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including) the provided message with this
     * transaction, it will store in transaction pending ack.
     *
     * <p>After the transaction commit, the end of previous transaction acked message until this transaction
     * acked message will actually ack.
     *
     * <p>After the transaction abort, the end of previous transaction acked message until this transaction
     * acked message will be redelivered to this consumer.
     *
     * <p>Cumulative acknowledge with transaction only support cumulative ack and now have not support individual and
     * cumulative ack sharing.
     *
     * <p>If cumulative ack with a transaction success, we can cumulative ack messageId with the same transaction
     * more than previous messageId.
     *
     * <p>It will not be allowed to cumulative ack with a transaction different from the previous one when the previous
     * transaction haven't commit or abort.
     *
     * <p>Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @param txn {@link Transaction} the transaction to cumulative ack
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws org.apache.pulsar.client.api.PulsarClientException.TransactionConflictException
     *             if the ack with messageId is less than the messageId in pending ack state or ack with transaction is
     *             different from the transaction in pending ack.
     * @throws org.apache.pulsar.client.api.PulsarClientException.NotAllowedException
     *             broker don't support transaction
     * @return {@link CompletableFuture} the future of the ack result
     *
     * @since 2.7.0
     */
    CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId,
                                                       Transaction txn);

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
     * Asynchronously acknowledge the consumption of a single message.
     *
     * @param message
     *            The {@code Message} to be acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeAsync(Message<?> message);

    /**
     * Asynchronously acknowledge the consumption of a single message.
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeAsync(MessageId messageId);

    /**
     * Asynchronously acknowledge the consumption of a single message, it will store in pending ack.
     *
     * <p>After the transaction commit, the message will actually ack.
     *
     * <p>After the transaction abort, the message will be redelivered.
     *
     * @param messageId {@link MessageId} to be individual acknowledged
     * @param txn {@link Transaction} the transaction to cumulative ack
     * @throws PulsarClientException.AlreadyClosedException
     *             if the consumer was already closed
     * @throws org.apache.pulsar.client.api.PulsarClientException.TransactionConflictException
     *             if the ack with messageId has been acked by another transaction
     * @throws org.apache.pulsar.client.api.PulsarClientException.NotAllowedException
     *             broker don't support transaction
     *             don't find batch size in consumer pending ack
     * @return {@link CompletableFuture} the future of the ack result
     *
     * @since 2.7.0
     */
    CompletableFuture<Void> acknowledgeAsync(MessageId messageId, Transaction txn);

    /**
     * Asynchronously acknowledge the consumption of {@link Messages}.
     *
     * @param messages
     *            The {@link Messages} to be acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeAsync(Messages<?> messages);

    /**
     * Asynchronously acknowledge the consumption of a list of message.
     * @param messageIdList
     * @return
     */
    CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList);

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
     * Asynchronously Acknowledge the reception of all the messages in the stream up to (and including) the provided
     * message.
     *
     * <p>Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
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
     * <p>Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * @param messageId
     *            The {@code MessageId} to be cumulatively acknowledged
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId);

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
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    void seek(MessageId messageId) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     */
    void seek(long timestamp) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message ID or message publish time.
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
     * Reset the subscription associated with this consumer to a specific message id.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(MessageId messageId);

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(long timestamp);

    /**
     * Get the last message id available for consume.
     *
     * @return the last message id.
     */
    MessageId getLastMessageId() throws PulsarClientException;

    /**
     * Get the last message id available for consume.
     *
     * @return a future that can be used to track the completion of the operation.
     */
    CompletableFuture<MessageId> getLastMessageIdAsync();

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
