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

import org.apache.pulsar.client.api.MessageId;

public class TopicMessageIdImpl implements MessageId {

    /** This topicPartitionName is get from ConsumerImpl, it contains partition part. */
    private final String topicPartitionName;
    private final String topicName;
    private final MessageId messageId;

    public TopicMessageIdImpl(String topicPartitionName, String topicName, MessageId messageId) {
        this.messageId = messageId;
        this.topicPartitionName = topicPartitionName;
        this.topicName = topicName;
    }

    /**
     * Get the topic name without partition part of this message.
     * @return the name of the topic on which this message was published
     */
    public String getTopicName() {
        return this.topicName;
    }

    /**
     * Get the topic name which contains partition part for this message.
     * @return the topic name which contains Partition part
     */
    public String getTopicPartitionName() {
        return this.topicPartitionName;
    }

    public MessageId getInnerMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        return messageId.toString();
    }

    @Override
    public byte[] toByteArray() {
        return messageId.toByteArray();
    }

    @Override
    public int hashCode() {
        return messageId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return messageId.equals(obj);
    }

    @Override
    public int compareTo(MessageId o) {
        return messageId.compareTo(o);
    }
}
