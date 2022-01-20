package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Progress;
import org.apache.pulsar.common.api.EncryptionContext;

import java.util.Map;
import java.util.Optional;


public class ProgressMessageImpl<T> implements Message<T> {

    private final Message<T> msg;

    public Progress progress;

    public ProgressMessageImpl(Message<T> msg, Progress progress) {
        this.msg = msg;
        this.progress = progress;
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
    public int size() {
        return 0;
    }

    @Override
    public T getValue() {
        return null;
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
    public long getSequenceId() {
        return 0;
    }

    @Override
    public String getProducerName() {
        return null;
    }

    @Override
    public boolean hasKey() {
        return false;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return false;
    }

    @Override
    public byte[] getKeyBytes() {
        return new byte[0];
    }

    @Override
    public boolean hasOrderingKey() {
        return false;
    }

    @Override
    public byte[] getOrderingKey() {
        return new byte[0];
    }

    @Override
    public String getTopicName() {
        return null;
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return Optional.empty();
    }

    @Override
    public int getRedeliveryCount() {
        return 0;
    }

    @Override
    public byte[] getSchemaVersion() {
        return new byte[0];
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public String getReplicatedFrom() {
        return null;
    }

    @Override
    public void release() {

    }

    @Override
    public boolean hasBrokerPublishTime() {
        return false;
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
        return Optional.empty();
    }

    @Override
    public boolean hasIndex() {
        return false;
    }

    @Override
    public Optional<Long> getIndex() {
        return Optional.empty();
    }
}
