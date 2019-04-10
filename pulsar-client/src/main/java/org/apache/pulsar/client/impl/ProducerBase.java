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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;

public abstract class ProducerBase<T> extends HandlerState implements Producer<T> {

    protected final CompletableFuture<Producer<T>> producerCreatedFuture;
    protected final ProducerConfigurationData conf;
    protected final Schema<T> schema;
    protected final ProducerInterceptors<T> interceptors;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors<T> interceptors) {
        super(client, topic);
        if(schema instanceof AutoProduceBytesSchema) {
            AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
            try {
                SchemaInfo schemaInfo = client.getSchemaProviderLoadingCache().get(topic).getLatestSchema();
                if (schemaInfo != null) {
                    autoProduceBytesSchema.setSchema(GenericSchemaImpl.of(schemaInfo));
                } else {
                    throw new RuntimeException("Can't get schema info from schema info provider for topic: " + topic);
                }
            } catch (ExecutionException e) {
                throw new RuntimeException("Can't get generic schema provider for topic:" + topic);
            }
            schema = autoProduceBytesSchema;
        }
        this.producerCreatedFuture = producerCreatedFuture;
        this.conf = conf;
        this.schema = schema;
        this.interceptors = interceptors;
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

    public CompletableFuture<MessageId> sendAsync(Message<T> message) {
        return internalSendAsync(message);
    }

    @Override
    public TypedMessageBuilder<T> newMessage() {
        return new TypedMessageBuilderImpl<>(this, schema);
    }

    abstract CompletableFuture<MessageId> internalSendAsync(Message<T> message);

    public MessageId send(Message<T> message) throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = internalSendAsync(message);

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to triggerFlush it out
                triggerFlush();
            }

            return sendFuture.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public void flush() throws PulsarClientException {
        try {
            flushAsync().get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarClientException) {
                throw (PulsarClientException) cause;
            } else {
                throw new PulsarClientException(cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    abstract void triggerFlush();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
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

    protected Message<T> beforeSend(Message<T> message) {
        if (interceptors != null) {
            return interceptors.beforeSend(this, message);
        } else {
            return message;
        }
    }

    protected void onSendAcknowledgement(Message<T> message, MessageId msgId, Throwable exception) {
        if (interceptors != null) {
            interceptors.onSendAcknowledgement(this, message, msgId, exception);
        }
    }

    @Override
    public String toString() {
        return "ProducerBase{" + "topic='" + topic + '\'' + '}';
    }
}
