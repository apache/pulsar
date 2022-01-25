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
package org.apache.pulsar.client.impl.v1;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl;

public class ProducerV1Impl implements Producer {

    private final ProducerImpl<byte[]> producer;

    public ProducerV1Impl(ProducerImpl<byte[]> producer) {
        this.producer = producer;
    }

    public void close() throws PulsarClientException {
        producer.close();
    }

    public CompletableFuture<Void> closeAsync() {
        return producer.closeAsync();
    }

    public void flush() throws PulsarClientException {
        producer.flush();
    }

    public CompletableFuture<Void> flushAsync() {
        return producer.flushAsync();
    }

    public long getLastSequenceId() {
        return producer.getLastSequenceId();
    }

    public ProducerStats getStats() {
        return producer.getStats();
    }

    public boolean isConnected() {
        return producer.isConnected();
    }

    public MessageId send(byte[] value) throws PulsarClientException {
        return producer.send(value);
    }

    public MessageId send(Message<byte[]> value) throws PulsarClientException {
        return producer.send(value);
    }

    public CompletableFuture<MessageId> sendAsync(byte[] arg0) {
        return producer.sendAsync(arg0);
    }

    public CompletableFuture<MessageId> sendAsync(Message<byte[]> arg0) {
        return producer.sendAsync(arg0);
    }

    @Override
    public String getTopic() {
        return producer.getTopic();
    }

    @Override
    public String getProducerName() {
        return producer.getProducerName();
    }
}
