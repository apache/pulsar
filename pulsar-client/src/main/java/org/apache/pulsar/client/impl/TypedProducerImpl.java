package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;

class TypedProducerImpl<T> implements Producer<T> {
    private final Producer<byte[]> untypedProducer;
    private final Codec<T> codec;

    TypedProducerImpl(Producer<byte[]> untypedProducer, Codec<T> codec) {
        this.untypedProducer = untypedProducer;
        this.codec = codec;
    }

    @Override
    public String getTopic() {
        return untypedProducer.getTopic();
    }

    @Override
    public String getProducerName() {
        return untypedProducer.getProducerName();
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return untypedProducer.send(codec.encode(message));
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        return untypedProducer.sendAsync(codec.encode(message));
    }

    @Override
    public MessageId send(Message message) throws PulsarClientException {
        return untypedProducer.send(message);
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(Message message) {
        return untypedProducer.sendAsync(message);
    }

    @Override
    public long getLastSequenceId() {
        return untypedProducer.getLastSequenceId();
    }

    @Override
    public ProducerStats getStats() {
        return untypedProducer.getStats();
    }

    @Override
    public void close() throws PulsarClientException {
        untypedProducer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedProducer.closeAsync();
    }
}
