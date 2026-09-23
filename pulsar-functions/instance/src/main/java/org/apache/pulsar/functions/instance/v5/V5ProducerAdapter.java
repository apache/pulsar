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
package org.apache.pulsar.functions.instance.v5;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Presents a V5 producer through the v4 {@link Producer} interface.
 *
 * <p>This lets the producer cache, the Pulsar sink and the context's output messages publish with the V5 client
 * without a second code path. The V5 client has no per-message schemas, so {@link #newMessage(Schema)} accepts
 * only the schema the producer was created with, and it has no transactions in this runtime, so
 * {@link #newMessage(Transaction)} is not supported.
 */
public class V5ProducerAdapter<T> implements Producer<T> {

    private final org.apache.pulsar.client.api.v5.Producer<T> producer;
    private final Schema<T> schema;

    public V5ProducerAdapter(org.apache.pulsar.client.api.v5.Producer<T> producer, Schema<T> schema) {
        this.producer = producer;
        this.schema = schema;
    }

    /** Returns the wrapped V5 producer. */
    public org.apache.pulsar.client.api.v5.Producer<T> v5Producer() {
        return producer;
    }

    @Override
    public String getTopic() {
        return producer.topic();
    }

    @Override
    public String getProducerName() {
        return producer.producerName();
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return newMessage().value(message).send();
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        return newMessage().value(message).sendAsync();
    }

    @Override
    public void flush() throws PulsarClientException {
        try {
            flushAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        return producer.async().flush();
    }

    @Override
    public TypedMessageBuilder<T> newMessage() {
        return new V5TypedMessageBuilder<>(producer.async().newMessage());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> TypedMessageBuilder<V> newMessage(Schema<V> messageSchema) {
        checkSchema(messageSchema);
        return (TypedMessageBuilder<V>) newMessage();
    }

    /**
     * Returns a V5 message builder for a message with the given schema, which must be the producer's schema.
     */
    public AsyncMessageBuilder<T> newMessageV5(Schema<?> messageSchema) {
        checkSchema(messageSchema);
        return producer.async().newMessage();
    }

    private void checkSchema(Schema<?> messageSchema) {
        if (messageSchema != null && messageSchema != schema
                && !Objects.equals(schemaInfo(messageSchema), schemaInfo(schema))) {
            throw new UnsupportedOperationException("The V5 client does not support per-message schemas: the "
                    + "producer for " + producer.topic() + " was created with schema " + schemaInfo(schema));
        }
    }

    @Override
    public TypedMessageBuilder<T> newMessage(Transaction txn) {
        throw new UnsupportedOperationException("Transactions are not supported with the V5 client");
    }

    @Override
    public <V> TypedMessageBuilder<V> newMessage(Schema<V> messageSchema, Transaction txn) {
        throw new UnsupportedOperationException("Transactions are not supported with the V5 client");
    }

    @Override
    public long getLastSequenceId() {
        return producer.lastSequenceId();
    }

    @Override
    public ProducerStats getStats() {
        throw new UnsupportedOperationException("Producer stats are not available with the V5 client");
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return producer.async().close();
    }

    @Override
    public boolean isConnected() {
        // a scalable topic producer reconnects its segment producers on its own
        return true;
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return 0;
    }

    @Override
    public int getNumOfPartitions() {
        return 0;
    }

    private static SchemaInfo schemaInfo(Schema<?> schema) {
        return schema != null ? schema.getSchemaInfo() : null;
    }
}
