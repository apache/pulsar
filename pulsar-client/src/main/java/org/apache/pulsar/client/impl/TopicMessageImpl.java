package org.apache.pulsar.client.impl;

import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

public class TopicMessageImpl implements Message {

    private final String topicName;
    private final Message msg;
    private final MessageId msgId;

    TopicMessageImpl(String topicName,
                     Message msg) {
        this.topicName = topicName;
        this.msg = msg;
        this.msgId = new TopicMessageIdImpl(topicName, msg.getMessageId());
    }

    @Override
    public String getTopicName() {
        return topicName;
    }

    @Override
    public Map<String, String> getProperties() {
        return msg.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return msg.hasProperty(name);
    }

    @Override
    public String getProperty(String name) {
        return msg.getProperty(name);
    }

    @Override
    public byte[] getData() {
        return msg.getData();
    }

    @Override
    public MessageId getMessageId() {
        return msgId;
    }

    @Override
    public long getPublishTime() {
        return msg.getPublishTime();
    }

    @Override
    public long getEventTime() {
        return msg.getEventTime();
    }

    @Override
    public long getSequenceId() {
        return msg.getSequenceId();
    }

    @Override
    public String getProducerName() {
        return msg.getProducerName();
    }

    @Override
    public boolean hasKey() {
        return msg.hasKey();
    }

    @Override
    public String getKey() {
        return msg.getKey();
    }
}
