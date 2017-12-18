package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

public class TypedConsumerConfigAdapter<T> implements ConsumerConfig<Message> {
    private final ConsumerConfig<TypedMessage<T>> typedConfig;
    private final Codec<T> codec;

    private TypedConsumerImpl<T> typedConsumer;

    TypedConsumerConfigAdapter(ConsumerConfig<TypedMessage<T>> typedConfig, Codec<T> codec) {
        this.typedConfig = typedConfig;
        this.codec = codec;
    }

    public void setTypedConsumer(TypedConsumerImpl<T> typedConsumer) {
        this.typedConsumer = typedConsumer;
    }

    @Override
    public long getAckTimeoutMillis() {
        return typedConfig.getAckTimeoutMillis();
    }

    @Override
    public SubscriptionType getSubscriptionType() {
        return typedConfig.getSubscriptionType();
    }

    @Override
    public MessageListener<Message> getMessageListener() {
        MessageListener<TypedMessage<T>> listener = typedConfig.getMessageListener();
        return new MessageListener<Message>() {
            @Override
            public void received(Consumer<Message> consumer, Message msg) {
                listener.received(typedConsumer, new TypedMessageImpl<>(msg, codec));
            }

            @Override
            public void reachedEndOfTopic(Consumer<Message> consumer) {
                listener.reachedEndOfTopic(typedConsumer);
            }
        };
    }

    @Override
    public int getReceiverQueueSize() {
        return typedConfig.getReceiverQueueSize();
    }

    @Override
    public CryptoKeyReader getCryptoKeyReader() {
        return typedConfig.getCryptoKeyReader();
    }

    @Override
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return typedConfig.getCryptoFailureAction();
    }

    @Override
    public String getConsumerName() {
        return typedConfig.getConsumerName();
    }

    @Override
    public int getPriorityLevel() {
        return typedConfig.getPriorityLevel();
    }
}
