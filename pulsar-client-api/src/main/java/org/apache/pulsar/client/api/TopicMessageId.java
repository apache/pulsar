/*
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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.internal.DefaultImplementation;

/**
 * The MessageId used for a consumer that subscribes multiple topics or partitioned topics.
 *
 * <p>
 * It's guaranteed that {@link Message#getMessageId()} must return a TopicMessageId instance if the Message is received
 * from a consumer that subscribes multiple topics or partitioned topics.
 * The topic name used in APIs related to this class like `getOwnerTopic` and `create` must be the full topic name. For
 * example, "my-topic" is invalid while "persistent://public/default/my-topic" is valid.
 * If the topic is a partitioned topic, the topic name should be the name of the specific partition, e.g.
 * "persistent://public/default/my-topic-partition-0".
 * </p>
 */
public interface TopicMessageId extends MessageId {

    /**
     * Return the owner topic name of a message.
     *
     * @return the owner topic
     */
    String getOwnerTopic();

    static TopicMessageId create(String topic, MessageId messageId) {
        if (messageId instanceof TopicMessageId) {
            return (TopicMessageId) messageId;
        }
        return DefaultImplementation.getDefaultImplementation().newTopicMessageId(topic, messageId);
    }
}
