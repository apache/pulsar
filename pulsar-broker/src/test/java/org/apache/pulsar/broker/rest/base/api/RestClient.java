package org.apache.pulsar.broker.rest.base.api;

import java.io.Closeable;

public interface RestClient extends Closeable {
    @Override
    void close();

    RestProducer producer();

    RestConsumer consumer();
}
