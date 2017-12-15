package org.apache.pulsar.client.api;

public interface Codec<T> {
    byte[] encode(T message);
    T decode(byte[] bytes);
}
