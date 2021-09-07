/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.client.impl;

import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.EncryptionContext;

public class TopicMessageImpl<T> implements Message<T> {

    /** This topicPartitionName is get from ConsumerImpl, it contains partition part. */
    private final String topicPartitionName;

    private final Message<T> msg;
    private final TopicMessageIdImpl messageId;
    // consumer if this message is received by that consumer
    final ConsumerImpl receivedByconsumer;

    TopicMessageImpl(String topicPartitionName,
                     String topicName,
                     Message<T> msg,
                     ConsumerImpl receivedByConsumer) {
        this.topicPartitionName = topicPartitionName;
        this.receivedByconsumer = receivedByConsumer;

        this.msg = msg;
        this.messageId = new TopicMessageIdImpl(topicPartitionName, topicName, msg.getMessageId());
    }

    /**
     * Get the topic name without partition part of this message.
     * @return the name of the topic on which this message was published
     */
    @Override
    public String getTopicName() {
        return msg.getTopicName();
    }

    /**
     * Get the topic name which contains partition part for this message.
     * @return the topic name which contains Partition part
     */
    public String getTopicPartitionName() {
        return topicPartitionName;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    public MessageId getInnerMessageId() {
        return messageId.getInnerMessageId();
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
    public int size() {
        return msg.size();
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

    @Override
    public boolean hasBase64EncodedKey() {
        return msg.hasBase64EncodedKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return msg.getKeyBytes();
    }

    @Override
    public boolean hasOrderingKey() {
        return msg.hasOrderingKey();
    }

    @Override
    public byte[] getOrderingKey() {
        return msg.getOrderingKey();
    }

    @Override
    public T getValue() {
        return msg.getValue();
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return msg.getEncryptionCtx();
    }

    @Override
    public int getRedeliveryCount() {
        return msg.getRedeliveryCount();
    }

    @Override
    public byte[] getSchemaVersion() {
        return msg.getSchemaVersion();
    }

    @Override
    public boolean isReplicated() {
        return msg.isReplicated();
    }

    @Override
    public String getReplicatedFrom() {
        return msg.getReplicatedFrom();
    }

    public Message<T> getMessage() {
        return msg;
    }

    public Schema<T> getSchemaInternal() {
        if (this.msg instanceof MessageImpl) {
            MessageImpl message = (MessageImpl) this.msg;
            return message.getSchemaInternal();
        }
        return null;
    }

    @Override
    public Optional<Schema<?>> getReaderSchema() {
        return msg.getReaderSchema();
    }

    @Override
    public void release() {
        msg.release();
    }

    @Override
    public boolean hasBrokerPublishTime() {
        return msg.hasBrokerPublishTime();
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
        return msg.getBrokerPublishTime();
    }

    @Override
    public boolean hasIndex() {
        return msg.hasIndex();
    }

    @Override
    public Optional<Long> getIndex() {
        return msg.getIndex();
    }
}
