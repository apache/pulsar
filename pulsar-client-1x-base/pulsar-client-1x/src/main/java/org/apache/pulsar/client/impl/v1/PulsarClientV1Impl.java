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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;

@SuppressWarnings("deprecation")
public class PulsarClientV1Impl implements PulsarClient {

    private final PulsarClientImpl client;

    public PulsarClientV1Impl(String serviceUrl, ClientConfiguration conf) throws PulsarClientException {
        this.client = new PulsarClientImpl(conf.setServiceUrl(serviceUrl).getConfigurationData().clone());
    }

    public void close() throws PulsarClientException {
        client.close();
    }

    public CompletableFuture<Void> closeAsync() {
        return client.closeAsync();
    }

    public Producer createProducer(String arg0,
            ProducerConfiguration arg1) throws PulsarClientException {
        return new ProducerV1Impl((ProducerImpl<byte[]>) client.createProducer(arg0, arg1));
    }

    public Producer createProducer(String arg0)
            throws PulsarClientException {
        return new ProducerV1Impl((ProducerImpl<byte[]>) client.createProducer(arg0));
    }

    public CompletableFuture<Producer> createProducerAsync(String arg0,
            ProducerConfiguration arg1) {
        return client.createProducerAsync(arg0, arg1).thenApply(p -> new ProducerV1Impl((ProducerImpl<byte[]>) p));
    }

    public CompletableFuture<Producer> createProducerAsync(String arg0) {
        return client.createProducerAsync(arg0).thenApply(p -> new ProducerV1Impl((ProducerImpl<byte[]>) p));
    }

    public Reader createReader(String arg0, MessageId arg1,
            ReaderConfiguration arg2) throws PulsarClientException {
        return new ReaderV1Impl(client.createReader(arg0, arg1, arg2));
    }

    public CompletableFuture<Reader> createReaderAsync(String arg0,
            MessageId arg1, ReaderConfiguration arg2) {
        return client.createReaderAsync(arg0, arg1, arg2).thenApply(r -> new ReaderV1Impl(r));
    }

    public CompletableFuture<List<String>> getPartitionsForTopic(String arg0) {
        return client.getPartitionsForTopic(arg0);
    }

    public void shutdown() throws PulsarClientException {
        client.shutdown();
    }

    public Consumer subscribe(String arg0, String arg1,
            ConsumerConfiguration arg2) throws PulsarClientException {
        return new ConsumerV1Impl(client.subscribe(arg0, arg1, arg2));
    }

    public Consumer subscribe(String arg0, String arg1) throws PulsarClientException {
        return new ConsumerV1Impl(client.subscribe(arg0, arg1));
    }

    public CompletableFuture<Consumer> subscribeAsync(String arg0,
            String arg1, ConsumerConfiguration arg2) {
        return client.subscribeAsync(arg0, arg1, arg2).thenApply(c -> new ConsumerV1Impl(c));
    }

    public CompletableFuture<Consumer> subscribeAsync(String arg0,
            String arg1) {
        return client.subscribeAsync(arg0, arg1).thenApply(c -> new ConsumerV1Impl(c));
    }

    public void updateServiceUrl(String arg0) throws PulsarClientException {
        client.updateServiceUrl(arg0);
    }

}
