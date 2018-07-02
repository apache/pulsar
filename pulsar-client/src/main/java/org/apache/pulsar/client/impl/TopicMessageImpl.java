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
import org.apache.pulsar.common.api.EncryptionContext;

public class TopicMessageImpl<T> extends MessageRecordImpl<T, TopicMessageIdImpl> {

    private final String topicName;
    private final Message<T> msg;

    TopicMessageImpl(String topicName,
                     Message<T> msg) {
        this.topicName = topicName;
        this.msg = msg;
        this.messageId = new TopicMessageIdImpl(topicName, msg.getMessageId());
    }

    /**
     * Get the topic name of this message.
     * @return the name of the topic on which this message was published
     */
    public String getTopicName() {
        return topicName;
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
    public T getValue() {
        return msg.getValue();
    }
    
    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return (msg instanceof MessageImpl) ? ((MessageImpl) msg).getEncryptionCtx() : Optional.empty();
    }
}
