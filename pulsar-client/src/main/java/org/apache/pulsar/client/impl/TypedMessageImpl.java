package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessage;

import java.util.Map;

public class TypedMessageImpl<T> implements TypedMessage<T> {
    private final Message message;

    public TypedMessageImpl(Message message) {
        this.message = message;
    }

    @Override
    public T getMessage() {
        return null;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean hasProperty(String name) {
        return false;
    }

    @Override
    public String getProperty(String name) {
        return null;
    }

    @Override
    public byte[] getData() {
        return new byte[0];
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public long getPublishTime() {
        return 0;
    }

    @Override
    public long getEventTime() {
        return 0;
    }

    @Override
    public boolean hasKey() {
        return false;
    }

    @Override
    public String getKey() {
        return null;
    }
}
