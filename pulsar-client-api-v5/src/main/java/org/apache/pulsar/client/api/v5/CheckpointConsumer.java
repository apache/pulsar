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

import java.io.Closeable;
import java.time.Duration;
import org.apache.pulsar.client.api.v5.async.AsyncCheckpointConsumer;

/**
 * An unmanaged consumer designed for connector frameworks (Flink, Spark, etc.).
 *
 * <p>Unlike {@link StreamConsumer} and {@link QueueConsumer}, this consumer has no
 * broker-managed subscription — position tracking is entirely external. The connector
 * framework stores checkpoints in its own state backend and uses them to restore
 * on failure.
 *
 * <p>Internally, the consumer reads from all hash-range segments of a topic.
 * {@link #checkpoint()} creates an atomic snapshot of positions across all segments,
 * returned as an opaque {@link Checkpoint} that can be serialized and stored externally.
 *
 * <p>This interface provides synchronous (blocking) operations. For non-blocking
 * usage, obtain an {@link AsyncCheckpointConsumer} via {@link #async()}.
 *
 * @param <T> the type of message values
 */
public interface CheckpointConsumer<T> extends Closeable {

    /**
     * The topic this consumer reads from.
     *
     * @return the fully qualified topic name
     */
    String topic();

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
     * Receive a batch of messages, blocking up to the given timeout.
     *
     * @param maxMessages the maximum number of messages to return
     * @param timeout     the maximum time to wait for messages
     * @return the received {@link Messages} batch
     * @throws PulsarClientException if the consumer is closed or a connection error occurs
     */
    Messages<T> receiveMulti(int maxMessages, Duration timeout) throws PulsarClientException;

    // --- Checkpoint ---

    /**
     * Create a consistent checkpoint — an atomic snapshot of positions across all
     * internal hash-range segments.
     *
     * <p>The returned {@link Checkpoint} can be serialized via {@link Checkpoint#toByteArray()}
     * and stored in the connector framework's state backend.
     *
     * @return an opaque {@link Checkpoint} representing the current read positions
     */
    Checkpoint checkpoint();

    // --- Async ---

    /**
     * Return the asynchronous view of this consumer.
     *
     * @return the {@link AsyncCheckpointConsumer} counterpart of this consumer
     */
    AsyncCheckpointConsumer<T> async();

    // --- Lifecycle ---

    /**
     * Close the consumer and release all resources.
     *
     * @throws PulsarClientException if an error occurs while closing the consumer
     */
    @Override
    void close() throws PulsarClientException;
}
