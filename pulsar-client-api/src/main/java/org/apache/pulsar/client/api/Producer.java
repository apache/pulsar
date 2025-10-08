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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Producer is used to publish messages on a topic.
 *
 * <p>A single producer instance can be used across multiple threads.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Producer<T> extends Closeable {

    /**
     * @return the topic which producer is publishing to
     */
    String getTopic();

    /**
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    String getProducerName();

    /**
     * Sends a message.
     *
     * <p>This call will be blocking until is successfully acknowledged by the Pulsar broker.
     *
     * <p>Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
     *
     * @param message
     *            a message
     * @return the message id assigned to the published message
     * @throws PulsarClientException.TimeoutException
     *             if the message was not correctly received by the system within the timeout period
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    MessageId send(T message) throws PulsarClientException;

    /**
     * Send a message asynchronously.
     *
     * <p>When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     *
     * <p>See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * <p>Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
     * <p>
     * Note: The returned future is completed in the internal network I/O thread. If a callback that takes a long time
     * to complete is registered on the future, it can negatively impact the internal network processing.
     * </p>
     *
     * For example, consider the following code snippet:
     * <pre>{@code
     * Producer<byte[]> producer1 = client.newProducer().topic("topic1").create();
     * CompletableFuture<MessageId> future = producer.sendAsync("hello".getBytes());
     * future.thenAccept(__ -> {
     *     try {
     *         Thread.sleep(1000 * 3600L); // Simulates a long-running task (1 hour)
     *     } catch (InterruptedException ignored) {
     *     }
     * });
     * future.get();
     * Producer<byte[]> producer2 = client.newProducer().topic("topic2").create();
     * }</pre>
     *
     * <p>
     * In this example, the creation of `producer2` could be blocked for 1 hour. This behavior might seem
     * counter-intuitive, but it occurs because the callback registered on the `future` is executed immediately in the
     * single network I/O thread after the `future` is completed. While the callback is running (e.g., sleeping for 1
     * hour), the I/O thread is unable to process any other network responses from the broker, causing a bottleneck.
     * </p>
     *
     * <p>
     * In addition, invoking any synchronous APIs within the callback of an asynchronous operation will lead to a
     * deadlock. For example:
     * </p>
     *
     * <pre>{@code
     * producer.sendAsync("msg-1".getBytes()).thenAccept(__ -> producer.send("msg-2".getBytes()));
     * }</pre>
     *
     * <p>
     * In the example above, the synchronous `send` method is called within the callback of the asynchronous `sendAsync`
     * method. This will cause a deadlock because the I/O thread responsible for completing the `sendAsync` operation is
     * blocked waiting for the synchronous `send` method to complete. As a result, "msg-2" will never be sent, and the
     * I/O thread will remain blocked indefinitely. This can have a cascading effect, impacting all producers and
     * consumers created by the same {@link PulsarClient}.
     * </p>
     *
     * <p>
     * To avoid issues above, you should ensure that callbacks are executed in a separate thread or executor. This can
     * be achieved by using the `xxxAsync` APIs, such as:
     * </p>
     *
     * <ul>
     *   <li>{@link CompletableFuture#thenAcceptAsync(Consumer, Executor)}</li>
     *   <li>{@link CompletableFuture#thenAcceptAsync(Consumer)}</li>
     * </ul>
     *
     * <p>
     * These methods allow you to specify an executor for the callback, ensuring that the network I/O thread remains
     * unblocked. Alternatively, you can ensure that the callback logic is lightweight and completes quickly.
     * </p>
     *
     * @param message
     *            a byte array with the payload of the message
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync(T message);

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
     *
     * @throws PulsarClientException
     * @since 2.1.0
     * @see #flushAsync()
     */
    void flush() throws PulsarClientException;

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
     *
     * @return a future that can be used to track when all the messages have been safely persisted.
     * @since 2.1.0
     * @see #flush()
     */
    CompletableFuture<Void> flushAsync();

    /**
     * Create a new message builder.
     *
     * <p>This message builder allows to specify additional properties on the message. For example:
     * <pre>{@code
     * producer.newMessage()
     *       .key(messageKey)
     *       .value(myValue)
     *       .property("user-defined-property", "value")
     *       .send();
     * }</pre>
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     */
    TypedMessageBuilder<T> newMessage();

    /**
     * Create a new message builder with schema, not required same parameterized type with the producer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     */
    <V> TypedMessageBuilder<V> newMessage(Schema<V> schema);

    /**
     * Create a new message builder with transaction.
     *
     * <p>After the transaction commit, it will be made visible to consumer.
     *
     * <p>After the transaction abort, it will never be visible to consumer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     *
     * @since 2.7.0
     */
    TypedMessageBuilder<T> newMessage(Transaction txn);

    /**
     * Create a new message builder with transaction and schema, not required same parameterized type with the
     * producer.
     *
     * <p>After the transaction commit, it will be made visible to consumer.
     *
     * <p>After the transaction abort, it will never be visible to consumer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     */
    <V> TypedMessageBuilder<V> newMessage(Schema<V> schema,
                                          Transaction txn);

    /**
     * Get the last sequence id that was published by this producer.
     *
     * <p>This represent either the automatically assigned
     * or custom sequence id (set on the {@link TypedMessageBuilder})
     * that was published and acknowledged by the broker.
     *
     * <p>After recreating a producer with the same producer name, this will return the last message that was
     * published in the previous producer session, or -1 if there no message was ever published.
     *
     * @return the last sequence id published by this producer
     */
    long getLastSequenceId();

    /**
     * Get statistics for the producer.
     * <ul>
     * <li>numMsgsSent : Number of messages sent in the current interval
     * <li>numBytesSent : Number of bytes sent in the current interval
     * <li>numSendFailed : Number of messages failed to send in the current interval
     * <li>numAcksReceived : Number of acks received in the current interval
     * <li>totalMsgsSent : Total number of messages sent
     * <li>totalBytesSent : Total number of bytes sent
     * <li>totalSendFailed : Total number of messages failed to send
     * <li>totalAcksReceived: Total number of acks received
     * </ul>
     *
     * @return statistic for the producer or null if ProducerStatsRecorderImpl is disabled.
     */
    ProducerStats getStats();

    /**
     * Close the producer and releases resources allocated.
     *
     * <p>No more writes will be accepted from this producer. Waits until all pending write request are persisted.
     * In case of errors, pending writes will not be retried.
     *
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Close the producer and releases resources allocated.
     *
     * <p>No more writes will be accepted from this producer. Waits until all pending write request are persisted.
     * In case of errors, pending writes will not be retried.
     *
     * @return a future that can used to track when the producer has been closed
     */
    CompletableFuture<Void> closeAsync();

    /**
     * @return Whether the producer is currently connected to the broker
     */
    boolean isConnected();

    /**
     * @return The last disconnected timestamp of the producer
     */
    long getLastDisconnectedTimestamp();

    /**
     * @return the number of partitions per topic.
     */
    int getNumOfPartitions();
}
