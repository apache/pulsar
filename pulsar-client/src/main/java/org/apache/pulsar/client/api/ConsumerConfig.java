package org.apache.pulsar.client.api;

import java.io.Serializable;

public interface ConsumerConfig<M extends Message> extends Serializable {
    long getAckTimeoutMillis();

    SubscriptionType getSubscriptionType();

    MessageListener<M> getMessageListener();

    int getReceiverQueueSize();

    CryptoKeyReader getCryptoKeyReader();

    ConsumerCryptoFailureAction getCryptoFailureAction();

    String getConsumerName();

    int getPriorityLevel();
}
