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
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;

public abstract class ProducerBase extends HandlerBase implements Producer {

    protected final CompletableFuture<Producer> producerCreatedFuture;
    protected final ProducerConfiguration conf;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfiguration conf,
            CompletableFuture<Producer> producerCreatedFuture) {
        super(client, topic);
        this.producerCreatedFuture = producerCreatedFuture;
        this.conf = conf;
    }

    @Override
    public MessageId send(byte[] message) throws PulsarClientException {
        return send(MessageBuilder.create().setContent(message).build());
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(byte[] message) {
        return sendAsync(MessageBuilder.create().setContent(message).build());
    }

    @Override
    public MessageId send(Message message) throws PulsarClientException {
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
    abstract public CompletableFuture<MessageId> sendAsync(Message message);

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

    public ProducerConfiguration getConfiguration() {
        return conf;
    }

    public CompletableFuture<Producer> producerCreatedFuture() {
        return producerCreatedFuture;
    }
}
