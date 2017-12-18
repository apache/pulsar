package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TypedReaderImpl<T> implements Reader<TypedMessage<T>> {

    private final Reader<Message> untypedReader;
    private final Codec<T> codec;

    TypedReaderImpl(Reader<Message> untypedReader, Codec<T> codec) {
        this.untypedReader = untypedReader;
        this.codec = codec;
    }

    @Override
    public String getTopic() {
        return untypedReader.getTopic();
    }

    @Override
    public TypedMessage<T> readNext() throws PulsarClientException {
        return new TypedMessageImpl<>(untypedReader.readNext(), codec);
    }

    @Override
    public TypedMessage<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        return new TypedMessageImpl<>(untypedReader.readNext(timeout, unit), codec);
    }

    @Override
    public CompletableFuture<Message> readNextAsync() {
        return untypedReader.readNextAsync().thenApply((message) ->
                new TypedMessageImpl<>(message, codec)
        );
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedReader.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return untypedReader.hasReachedEndOfTopic();
    }

    @Override
    public void close() throws IOException {
        untypedReader.close();
    }
}
