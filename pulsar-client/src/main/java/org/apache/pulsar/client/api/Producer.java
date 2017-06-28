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

import org.apache.pulsar.client.impl.ProducerStats;

/**
 * Producer object.
 * 
 * The producer is used to publish messages on a topic
 *
 *
 */
public interface Producer extends Closeable {

    /**
     * @return the topic which producer is publishing to
     */
    String getTopic();

    /**
     * Send a message
     *
     * @param message
     *            a byte array with the payload of the message
     * @return the message id assigned to the published message
     * @throws PulsarClientException.TimeoutException
     *             if the message was not correctly received by the system within the timeout period
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    MessageId send(byte[] message) throws PulsarClientException;

    /**
     * Send a message asynchronously
     * <p>
     * When the producer queue is full, by default this method will complete the future with an exception {@link PulsarClientException#ProducerQueueIsFullError}
     * <p>
     * See {@link ProducerConfiguration#setMaxPendingMessages} to configure the producer queue size and
     * {@link ProducerConfiguration#setBlockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * @param message
     *            a byte array with the payload of the message
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync(byte[] message);

    /**
     * Send a message
     *
     * @param message
     *            a message
     * @return the message id assigned to the published message
     * @throws PulsarClientException.TimeoutException
     *             if the message was not correctly received by the system within the timeout period
     */
    MessageId send(Message message) throws PulsarClientException;

    /**
     * Send a message asynchronously
     * <p>
     * When the returned {@link CompletatableFuture} is marked as completed successfully, the provided message will
     * contain the {@link MessageId} assigned by the broker to the published message.
     * <p>
     * Example:
     * 
     * <pre>
     * <code>Message msg = MessageBuilder.create().setContent(myContent).build();
     * producer.sendAsync(msg).thenRun(v -> {
     *    System.out.println("Published message: " + msg.getMessageId());
     * }).exceptionally(e -> {
     *    // Failed to publish
     * });</code>
     * </pre>
     * <p>
     * When the producer queue is full, by default this method will complete the future with an exception {@link PulsarClientException#ProducerQueueIsFullError}
     * <p>
     * See {@link ProducerConfiguration#setMaxPendingMessages} to configure the producer queue size and
     * {@link ProducerConfiguration#setBlockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * @param message
     *            a message
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync(Message message);

    /**
     * Get statistics for the producer
     * 
     * numMsgsSent : Number of messages sent in the current interval numBytesSent : Number of bytes sent in the current
     * interval numSendFailed : Number of messages failed to send in the current interval numAcksReceived : Number of
     * acks received in the current interval totalMsgsSent : Total number of messages sent totalBytesSent : Total number
     * of bytes sent totalSendFailed : Total number of messages failed to send totalAcksReceived: Total number of acks
     * received
     * 
     * @return statistic for the producer or null if ProducerStats is disabled.
     */
    ProducerStats getStats();

    /**
     * Close the producer and releases resources allocated.
     * 
     * No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
     * of errors, pending writes will not be retried.
     * 
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Close the producer and releases resources allocated.
     * 
     * No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
     * of errors, pending writes will not be retried.
     * 
     * @return a future that can used to track when the producer has been closed
     */
    CompletableFuture<Void> closeAsync();
}
