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
        return untypedClient.createProducerAsync(topic).thenApply((producer -> {
            ProducerImpl<byte[]> impl = (ProducerImpl<byte[]>) producer;
            return new TypedProducerImpl<>(impl, codec);
        }));
    }

    @Override
    public Producer<T> createProducer(String topic, ProducerConfiguration conf) throws PulsarClientException {
        return new TypedProducerImpl<>(untypedClient.createProducer(topic, conf), codec);
    }

    @Override
    public CompletableFuture<Producer<T>> createProducerAsync(String topic, ProducerConfiguration conf) {
        return untypedClient.createProducerAsync(topic, conf).thenApply((producer) -> {
            ProducerImpl<byte[]> impl = (ProducerImpl<byte[]>) producer;
            return new TypedProducerImpl<>(impl, codec);
        });
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription) throws PulsarClientException {
        return new TypedConsumerImpl<>((ConsumerImpl<Message>) untypedClient.subscribe(topic, subscription), codec);
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription) {
        return untypedClient.subscribeAsync(topic, subscription).thenApply((consumer) -> {
            ConsumerImpl<Message> impl = (ConsumerImpl<Message>) consumer;
            return new TypedConsumerImpl<>(impl, codec);
        });
    }

    @Override
    public Consumer<TypedMessage<T>> subscribe(String topic, String subscription, ConsumerConfiguration conf) throws PulsarClientException {
        return new TypedConsumerImpl<>((ConsumerImpl<Message>) untypedClient.subscribe(topic, subscription, conf), codec);
    }

    @Override
    public CompletableFuture<Consumer<TypedMessage<T>>> subscribeAsync(String topic, String subscription, ConsumerConfiguration conf) {
        return untypedClient.subscribeAsync(topic, subscription, conf).thenApply((consumer) ->{
            ConsumerImpl<Message> impl = (ConsumerImpl<Message>) consumer;
            return new TypedConsumerImpl<>(impl, codec);
        });
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
