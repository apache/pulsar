package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class TypedConsumerImpl<T> implements Consumer<TypedMessage<T>> {

    TypedConsumerImpl(ConsumerImpl<Message> untypedConsumer) {
        this.untypedConsumer = untypedConsumer;
    }

    private final ConsumerImpl<Message> untypedConsumer;

    @Override
    public String getTopic() {
        return untypedConsumer.getTopic();
    }

    @Override
    public String getSubscription() {
        return untypedConsumer.getSubscription();
    }

    @Override
    public void unsubscribe() throws PulsarClientException {
        untypedConsumer.unsubscribe();
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        return untypedConsumer.unsubscribeAsync();
    }

    @Override
    public TypedMessage<T> receive() throws PulsarClientException {
        return new TypedMessageImpl<>(untypedConsumer.receive());
    }

    @Override
    public CompletableFuture<TypedMessage<T>> receiveAsync() {
        return untypedConsumer.receiveAsync().thenApply((message) -> {
            MessageImpl impl = (MessageImpl) message;
            return new TypedMessageImpl<>(impl);
        });
    }

    @Override
    public TypedMessage<T> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        return new TypedMessageImpl<>(untypedConsumer.receive());
    }

    @Override
    public void acknowledge(TypedMessage<T> message) throws PulsarClientException {
        untypedConsumer.acknowledge(message);
    }

    @Override
    public void acknowledge(MessageId messageId) throws PulsarClientException {
        untypedConsumer.acknowledge(messageId);
    }

    @Override
    public void acknowledgeCumulative(TypedMessage<T> message) throws PulsarClientException {
        untypedConsumer.acknowledgeCumulative(message);
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
        untypedConsumer.acknowledgeCumulative(messageId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(TypedMessage<T> message) {
        return untypedConsumer.acknowledgeAsync(message);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return untypedConsumer.acknowledgeAsync(messageId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(TypedMessage<T> message) {
        return untypedConsumer.acknowledgeCumulativeAsync(message);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        return untypedConsumer.acknowledgeCumulativeAsync(messageId);
    }

    @Override
    public ConsumerStats getStats() {
        return untypedConsumer.getStats();
    }

    @Override
    public void close() throws PulsarClientException {
        untypedConsumer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedConsumer.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return untypedConsumer.hasReachedEndOfTopic();
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        untypedConsumer.redeliverUnacknowledgedMessages();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        untypedConsumer.seek(messageId);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return untypedConsumer.seekAsync(messageId);
    }
}
