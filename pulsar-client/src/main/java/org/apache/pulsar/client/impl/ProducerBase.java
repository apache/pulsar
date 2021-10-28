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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

public abstract class ProducerBase<T> extends HandlerState implements Producer<T> {

    protected final CompletableFuture<Producer<T>> producerCreatedFuture;
    protected final ProducerConfigurationData conf;
    protected final Schema<T> schema;
    protected final ProducerInterceptors interceptors;
    protected final ConcurrentOpenHashMap<SchemaHash, byte[]> schemaCache;
    protected volatile MultiSchemaMode multiSchemaMode = MultiSchemaMode.Auto;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors interceptors) {
        super(client, topic);
        this.producerCreatedFuture = producerCreatedFuture;
        this.conf = conf;
        this.schema = schema;
        this.interceptors = interceptors;
        this.schemaCache = new ConcurrentOpenHashMap<>();
        if (!conf.isMultiSchema()) {
            multiSchemaMode = MultiSchemaMode.Disabled;
        }
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return newMessage().value(message).send();
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        try {
            return newMessage().value(message).sendAsync();
        } catch (SchemaSerializationException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    public CompletableFuture<MessageId> sendAsync(Message<?> message) {
        return internalSendAsync(message);
    }

    @Override
    public TypedMessageBuilder<T> newMessage() {
        return new TypedMessageBuilderImpl<>(this, schema);
    }

    public <V> TypedMessageBuilder<V> newMessage(Schema<V> schema) {
        checkArgument(schema != null);
        return new TypedMessageBuilderImpl<>(this, schema);
    }

    @Override
    public TypedMessageBuilder<T> newMessage(Transaction txn) {
        checkArgument(txn instanceof TransactionImpl);

        // check the producer has proper settings to send transactional messages
        if (conf.getSendTimeoutMs() > 0) {
            throw new IllegalArgumentException("Only producers disabled sendTimeout are allowed to"
                + " produce transactional messages");
        }

        return new TypedMessageBuilderImpl<>(this, schema, (TransactionImpl) txn);
    }

    abstract CompletableFuture<MessageId> internalSendAsync(Message<?> message);

    abstract CompletableFuture<MessageId> internalSendWithTxnAsync(Message<?> message, Transaction txn);

    public MessageId send(Message<?> message) throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = internalSendAsync(message);

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to triggerFlush it out
                triggerFlush();
            }

            return sendFuture.get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void flush() throws PulsarClientException {
        try {
            flushAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    abstract void triggerFlush();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    abstract public CompletableFuture<Void> closeAsync();

    @Override
    public String getTopic() {
        return topic;
    }

    public ProducerConfigurationData getConfiguration() {
        return conf;
    }

    public CompletableFuture<Producer<T>> producerCreatedFuture() {
        return producerCreatedFuture;
    }

    protected Message<?> beforeSend(Message<?> message) {
        if (interceptors != null) {
            return interceptors.beforeSend(this, message);
        } else {
            return message;
        }
    }

    protected void onSendAcknowledgement(Message<?> message, MessageId msgId, Throwable exception) {
        if (interceptors != null) {
            interceptors.onSendAcknowledgement(this, message, msgId, exception);
        }
    }

    protected void onPartitionsChange(String topicName, int partitions) {
        if (interceptors != null) {
            interceptors.onPartitionsChange(topicName, partitions);
        }
    }

    @Override
    public String toString() {
        return "ProducerBase{" + "topic='" + topic + '\'' + '}';
    }

    public enum MultiSchemaMode {
        Auto, Enabled, Disabled
    }
}
