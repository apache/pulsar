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
package org.apache.pulsar.client.api.v5;

import java.time.Duration;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;

/**
 * A consumer for queue (unordered) consumption with broker-managed position tracking.
 *
 * <p>Messages are distributed to available consumers for parallel processing.
 * Acknowledgment is individual: each message must be acknowledged separately.
 *
 * <p>This interface provides synchronous (blocking) operations. For non-blocking
 * usage, obtain an {@link AsyncQueueConsumer} via {@link #async()}.
 *
 * <p>This maps to the Shared/Key_Shared subscription model in the Pulsar v4 API.
 *
 * @param <T> the type of message values
 */
public interface QueueConsumer<T> extends AutoCloseable {

    /**
     * The topic this consumer is subscribed to.
     *
     * @return the fully qualified topic name
     */
    String topic();

    /**
     * The subscription name.
     *
     * @return the subscription name
     */
    String subscription();

    /**
     * The consumer name (system-assigned or user-specified).
     *
     * @return the consumer name, never {@code null}
     */
    String consumerName();

    // --- Receive ---

    /**
     * Receive a single message, blocking indefinitely.
     *
     * @return the received {@link Message}
     * @throws PulsarClientException if the consumer is closed or a connection error occurs
     */
    Message<T> receive() throws PulsarClientException;

    /**
     * Receive a single message, blocking up to the given timeout.
     * Returns {@code null} if the timeout elapses without a message.
     *
     * @param timeout the maximum time to wait for a message
     * @return the received {@link Message}, or {@code null} if the timeout elapses
     * @throws PulsarClientException if the consumer is closed or a connection error occurs
     */
    Message<T> receive(Duration timeout) throws PulsarClientException;

    /**
     * Acknowledge a single message by its ID.
     *
     * @param messageId the ID of the message to acknowledge
     */
    void acknowledge(MessageId messageId);

    /**
     * Acknowledge within a transaction. The acknowledgment becomes effective when the
     * transaction is committed.
     *
     * @param messageId the ID of the message to acknowledge
     * @param txn       the transaction to associate the acknowledgment with
     */
    void acknowledge(MessageId messageId, Transaction txn);

    /**
     * Signal that the message with this ID could not be processed.
     *
     * @param messageId the ID of the message to negatively acknowledge
     */
    void negativeAcknowledge(MessageId messageId);

    /**
     * Return the asynchronous view of this consumer.
     *
     * @return the {@link AsyncQueueConsumer} counterpart of this consumer
     */
    AsyncQueueConsumer<T> async();

    /**
     * Close the consumer and release all resources.
     *
     * @throws PulsarClientException if an error occurs while closing the consumer
     */
    @Override
    void close() throws PulsarClientException;
}
