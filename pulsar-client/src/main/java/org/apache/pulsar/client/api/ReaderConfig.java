package org.apache.pulsar.client.api;

import java.io.Serializable;

public interface ReaderConfig<M extends Message> extends Serializable {
    ReaderListener<M> getReaderListener();

    int getReceiverQueueSize();

    ConsumerCryptoFailureAction getCryptoFailureAction();

    CryptoKeyReader getCryptoKeyReader();

    String getReaderName();
}
