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

import java.util.concurrent.CompletableFuture;
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

    PulsarClientV5(PulsarClientImpl v4Client, String description) {
        this.v4Client = v4Client;
        this.description = description;
    }

    /**
     * Get the underlying v4 client. Package-private for use by internal components.
     */
    PulsarClientImpl v4Client() {
        return v4Client;
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
        throw new PulsarClientException("Transactions not yet implemented");
    }

    @Override
    public CompletableFuture<Transaction> newTransactionAsync() {
        return CompletableFuture.failedFuture(
                new PulsarClientException("Transactions not yet implemented"));
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            v4Client.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return v4Client.closeAsync().exceptionally(ex -> {
            throw new java.util.concurrent.CompletionException(
                    new PulsarClientException(ex.getMessage(), ex));
        });
    }

    @Override
    public void shutdown() {
        try {
            v4Client.shutdown();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
