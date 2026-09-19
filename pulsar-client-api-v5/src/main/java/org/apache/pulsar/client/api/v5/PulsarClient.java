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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Entry point for the Pulsar client. Provides factory methods for creating producers,
 * consumers, and transactions.
 *
 * <p>Instances are created via {@link #builder()}.
 *
 * <p>A {@code PulsarClient} manages internal resources such as connections, threads,
 * and memory buffers. It must be closed when no longer needed.
 */
public interface PulsarClient extends AutoCloseable {

    /**
     * Create a new client builder.
     *
     * @return a new {@link PulsarClientBuilder} for configuring the client
     */
    static PulsarClientBuilder builder() {
        return PulsarClientProvider.get().newClientBuilder();
    }

    /**
     * Create a producer builder with a specific schema.
     *
     * @param <T> the message value type
     * @param schema the schema used for serialization/deserialization
     * @return a new {@link ProducerBuilder} for configuring the producer
     */
    <T> ProducerBuilder<T> newProducer(Schema<T> schema);

    /**
     * Create a stream consumer builder with a specific schema.
     *
     * @param <T> the message value type
     * @param schema the schema used for serialization/deserialization
     * @return a new {@link StreamConsumerBuilder} for configuring the stream consumer
     */
    <T> StreamConsumerBuilder<T> newStreamConsumer(Schema<T> schema);

    /**
     * Create a queue consumer builder with a specific schema.
     *
     * @param <T> the message value type
     * @param schema the schema used for serialization/deserialization
     * @return a new {@link QueueConsumerBuilder} for configuring the queue consumer
     */
    <T> QueueConsumerBuilder<T> newQueueConsumer(Schema<T> schema);

    /**
     * Create a checkpoint consumer builder with a specific schema.
     *
     * <p>Checkpoint consumers are unmanaged — position tracking is external.
     * Designed for connector frameworks (Flink, Spark) that manage their own state.
     *
     * @param <T> the message value type
     * @param schema the schema used for serialization/deserialization
     * @return a new {@link CheckpointConsumerBuilder} for configuring the checkpoint consumer
     */
    <T> CheckpointConsumerBuilder<T> newCheckpointConsumer(Schema<T> schema);

    // --- Transactions ---

    /**
     * Create a new transaction, blocking until it is ready. The transaction timeout is taken
     * from the client-wide {@link org.apache.pulsar.client.api.v5.config.TransactionPolicy}
     * configured on {@link PulsarClientBuilder#transactionPolicy}.
     *
     * @return a new {@link Transaction} in the {@link Transaction.State#OPEN} state
     * @throws PulsarClientException if the transaction cannot be created (e.g., transaction
     *         coordinator unavailable or the client is closed)
     */
    Transaction newTransaction() throws PulsarClientException;

    /**
     * Asynchronous counterpart of {@link #newTransaction()}.
     *
     * @return a {@link CompletableFuture} that completes with a new {@link Transaction} in the
     *         {@link Transaction.State#OPEN} state, or completes exceptionally with
     *         {@link PulsarClientException} on failure
     */
    CompletableFuture<Transaction> newTransactionAsync();

    // --- Lifecycle ---

    /**
     * Close the client and release all resources, waiting for pending operations to complete.
     *
     * @throws PulsarClientException if an error occurs while closing the client
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Asynchronous counterpart of {@link #close()}.
     *
     * @return a {@link CompletableFuture} that completes when the client has finished closing,
     *         or completes exceptionally with {@link PulsarClientException} on failure
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Shutdown the client instance.
     *
     * <p>Release all resources used by the client, without waiting for pending operations to complete.
     */
    void shutdown();
}
