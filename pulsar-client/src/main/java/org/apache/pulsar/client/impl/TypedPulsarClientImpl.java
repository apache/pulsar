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

import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;

public class TypedPulsarClientImpl<T> implements PulsarClient<T, TypedMessage<T>> {

    private final PulsarClientImpl untypedClient;
    private final Codec<T> codec;

    public TypedPulsarClientImpl(PulsarClientImpl untypedClient, Codec<T> codec) {
        this.untypedClient = untypedClient;
        this.codec = codec;
    }

    @Override
    public Producer<T> createProducer(String topic) throws PulsarClientException {
        return new TypedProducerImpl<>(untypedClient.createProducer(topic), codec);
    }

    @Override
    public CompletableFuture<Producer<T>> createProducerAsync(String topic) {
        return untypedClient.createProducerAsync(topic).thenApply((producer ->
                new TypedProducerImpl<>(producer, codec))
        );
    }

    @Override
    public Producer<T> createProducer(String topic, ProducerConfiguration conf) throws PulsarClientException {
        return new TypedProducerImpl<>(untypedClient.createProducer(topic, conf), codec);
    }

    @Override
    public CompletableFuture<Producer<T>> createProducerAsync(String topic, ProducerConfiguration conf) {
        return untypedClient.createProducerAsync(topic, conf).thenApply((producer) ->
                new TypedProducerImpl<>(producer, codec)
        );
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription) throws PulsarClientException {
        return new TypedConsumerImpl<>(untypedClient.subscribe(topic, subscription), codec);
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription) {
        return untypedClient.subscribeAsync(topic, subscription).thenApply((consumer) ->
                new TypedConsumerImpl<>(consumer, codec)
        );
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription, ConsumerConfig<TypedMessage<T>> conf) throws PulsarClientException {
        TypedConsumerConfigAdapter<T> adapted = new TypedConsumerConfigAdapter<>(conf, codec);
        TypedConsumerImpl<T> typedConsumer = new TypedConsumerImpl<>(untypedClient.subscribe(topic, subscription, adapted), codec);
        adapted.setTypedConsumer(typedConsumer);
        return typedConsumer;
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription, ConsumerConfig<TypedMessage<T>> conf) {
        final TypedConsumerConfigAdapter<T> adapted = new TypedConsumerConfigAdapter<>(conf, codec);
        return untypedClient.subscribeAsync(topic, subscription, adapted).thenApply((consumer) -> {
            TypedConsumerImpl<T> typedConsumer = new TypedConsumerImpl<>(consumer, codec);
            adapted.setTypedConsumer(typedConsumer);
            return typedConsumer;
        });
    }

    @Override
    public Reader<TypedMessage<T>> createReader(String topic, MessageId startMessageId, ReaderConfig<TypedMessage<T>> conf) throws PulsarClientException {
        TypedReaderConfigAdapter<T> adapted = new TypedReaderConfigAdapter<>(conf, codec);
        TypedReaderImpl<T> typedReader = new TypedReaderImpl<>(untypedClient.createReader(topic, startMessageId, adapted), codec);
        adapted.setTypedReader(typedReader);
        return typedReader;
    }

    @Override
    public CompletableFuture<Reader<TypedMessage<T>>> createReaderAsync(String topic, MessageId startMessageId, ReaderConfig<TypedMessage<T>> conf) {
        final TypedReaderConfigAdapter<T> adapted = new TypedReaderConfigAdapter<>(conf, codec);
        return untypedClient.createReaderAsync(topic, startMessageId, adapted).thenApply((reader) -> {
            TypedReaderImpl<T> typedReader = new TypedReaderImpl<>(reader, codec);
            adapted.setTypedReader(typedReader);
            return typedReader;
        });
    }

    @Override
    public void close() throws PulsarClientException {
        untypedClient.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedClient.closeAsync();
    }

    @Override
    public void shutdown() throws PulsarClientException {
        untypedClient.shutdown();
    }
}