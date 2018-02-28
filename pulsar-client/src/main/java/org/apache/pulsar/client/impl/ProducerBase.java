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
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

public abstract class ProducerBase<T> extends HandlerBase implements Producer<T> {

    protected final CompletableFuture<Producer<T>> producerCreatedFuture;
    protected final ProducerConfigurationData conf;
    protected final Schema<T> schema;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema) {
        super(client, topic, new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS,
                Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS));
        this.producerCreatedFuture = producerCreatedFuture;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return send(MessageBuilder.create(schema).setValue(message).build());
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        return sendAsync(MessageBuilder.create(schema).setValue(message).build());
    }

    @Override
    public MessageId send(Message<T> message) throws PulsarClientException {
        try {
            return sendAsync(message).get();
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
    abstract public CompletableFuture<MessageId> sendAsync(Message<T> message);

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

    abstract public boolean isConnected();

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

    @Override
    public String toString() {
        return "ProducerBase{" +
                "topic='" + topic + '\'' +
                '}';
    }
}
