package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

class TypedReaderConfigAdapter<T> implements ReaderConfig<Message> {
    private final ReaderConfig<TypedMessage<T>> typedConfig;
    private final Codec<T> codec;

    private TypedReaderImpl<T> typedReader;

    public TypedReaderConfigAdapter(ReaderConfig<TypedMessage<T>> typedConfig, Codec<T> codec) {
        this.typedConfig = typedConfig;
        this.codec = codec;
    }

    void setTypedReader(TypedReaderImpl<T> typedReader) {
        this.typedReader = typedReader;
    }

    @Override
    public ReaderListener<Message> getReaderListener() {
        final ReaderListener<TypedMessage<T>> listener = typedConfig.getReaderListener();
        return new ReaderListener<Message>() {
            @Override
            public void received(Reader<Message> ignore, Message msg) {
                listener.received(typedReader, new TypedMessageImpl<>(msg, codec));
            }

            @Override
            public void reachedEndOfTopic(Reader<Message> ignore) {
                listener.reachedEndOfTopic(typedReader);
            }
        };
    }

    @Override
    public int getReceiverQueueSize() {
        return typedConfig.getReceiverQueueSize();
    }

    @Override
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return typedConfig.getCryptoFailureAction();
    }

    @Override
    public CryptoKeyReader getCryptoKeyReader() {
        return typedConfig.getCryptoKeyReader();
    }

    @Override
    public String getReaderName() {
        return typedConfig.getReaderName();
    }
}
