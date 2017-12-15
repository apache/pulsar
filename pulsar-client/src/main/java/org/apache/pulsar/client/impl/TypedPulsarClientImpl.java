package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;

public class TypedPulsarClientImpl<T> implements PulsarClient<TypedMessage<T>> {

    private final PulsarClientImpl<Message> untypedClient;

    public TypedPulsarClientImpl(PulsarClientImpl<Message> untypedClient) {
        this.untypedClient = untypedClient;
    }

    @Override
    public Producer<Message> createProducer(String topic) throws PulsarClientException {
        return null;
    }

    @Override
    public CompletableFuture<Producer<Message>> createProducerAsync(String topic) {
        return null;
    }

    @Override
    public Producer<Message> createProducer(String topic, ProducerConfiguration conf) throws PulsarClientException {
        return null;
    }

    @Override
    public CompletableFuture<Producer<Message>> createProducerAsync(String topic, ProducerConfiguration conf) {
        return null;
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription) throws PulsarClientException {
        return new TypedConsumerImpl<>((ConsumerImpl<Message>) untypedClient.subscribe(topic, subscription));
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription) {
        return untypedClient.subscribeAsync(topic, subscription).thenApply((consumer) -> {
            ConsumerImpl<Message> impl = (ConsumerImpl<Message>) consumer;
            return new TypedConsumerImpl<>(impl);
        });
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription, ConsumerConfiguration conf) throws PulsarClientException {
        return new TypedConsumerImpl<>((ConsumerImpl<Message>) untypedClient.subscribe(topic, subscription, conf));
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription, ConsumerConfiguration conf) {
        return null;
    }

    @Override
    public Reader createReader(String topic, MessageId startMessageId, ReaderConfiguration conf) throws PulsarClientException {
        return null;
    }

    @Override
    public CompletableFuture<Reader> createReaderAsync(String topic, MessageId startMessageId, ReaderConfiguration conf) {
        return null;
    }

    @Override
    public void close() throws PulsarClientException {

    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    @Override
    public void shutdown() throws PulsarClientException {

    }
}
