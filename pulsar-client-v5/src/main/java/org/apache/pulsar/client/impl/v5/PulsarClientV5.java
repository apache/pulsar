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
package org.apache.pulsar.client.impl.v5;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.StreamConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;

/**
 * V5 PulsarClient implementation that wraps the v4 PulsarClientImpl for
 * connection management and transport. Adds scalable topic routing on top.
 */
final class PulsarClientV5 implements PulsarClient {

    private final PulsarClientImpl v4Client;
    private final String description;
    private final Duration transactionTimeout;

    /**
     * The V5 producers on scalable topics, until their close completes. Closing the v4 client closes
     * the per-segment v4 producers but not these, whose sends waiting for a new layout would then
     * never complete: they are closed first, and the v4 client once they are, including those already
     * closing.
     */
    private final Set<ScalableTopicProducer<?>> producers = ConcurrentHashMap.newKeySet();

    PulsarClientV5(PulsarClientImpl v4Client, String description, Duration transactionTimeout) {
        this.v4Client = v4Client;
        this.description = description;
        this.transactionTimeout = transactionTimeout;
    }

    /**
     * Get the underlying v4 client. Package-private for use by internal components.
     */
    PulsarClientImpl v4Client() {
        return v4Client;
    }

    void registerProducer(ScalableTopicProducer<?> producer) {
        producers.add(producer);
    }

    void deregisterProducer(ScalableTopicProducer<?> producer) {
        producers.remove(producer);
    }

    /** Close the V5 producers, before the v4 client and its executors. Never fails. */
    private CompletableFuture<Void> closeProducersAsync() {
        return CompletableFuture.allOf(producers.stream()
                .map(producer -> producer.closeAsync().exceptionally(__ -> null))
                .toArray(CompletableFuture[]::new));
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        return new ProducerBuilderV5<>(this, schema);
    }

    @Override
    public <T> StreamConsumerBuilder<T> newStreamConsumer(Schema<T> schema) {
        return new StreamConsumerBuilderV5<>(this, schema);
    }

    @Override
    public <T> QueueConsumerBuilder<T> newQueueConsumer(Schema<T> schema) {
        return new QueueConsumerBuilderV5<>(this, schema);
    }

    @Override
    public <T> CheckpointConsumerBuilder<T> newCheckpointConsumer(Schema<T> schema) {
        return new CheckpointConsumerBuilderV5<>(this, schema);
    }

    @Override
    public Transaction newTransaction() throws PulsarClientException {
        try {
            return newTransactionAsync().get();
        } catch (ExecutionException e) {
            throw V5ExceptionMappings.toV5Exception(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Interrupted while creating transaction", e);
        }
    }

    @Override
    public CompletableFuture<Transaction> newTransactionAsync() {
        var builder = v4Client.newTransaction();
        if (transactionTimeout != null) {
            builder.withTransactionTimeout(transactionTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        return builder.build().thenApply(v4Txn -> (Transaction) new TransactionV5(v4Txn))
                .exceptionallyCompose(V5ExceptionMappings::failWithV5Exception);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeProducersAsync().get();
        } catch (InterruptedException e) {
            // Close the v4 client all the same.
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // Not reached: closeProducersAsync never fails.
        }
        try {
            v4Client.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw V5ExceptionMappings.toV5Exception(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeProducersAsync().thenCompose(__ -> v4Client.closeAsync())
                .exceptionallyCompose(V5ExceptionMappings::failWithV5Exception);
    }

    @Override
    public void shutdown() {
        // Without waiting: this fails the sends waiting for a layout right away, and the v4 shutdown
        // fails the ones in flight.
        closeProducersAsync();
        try {
            v4Client.shutdown();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
