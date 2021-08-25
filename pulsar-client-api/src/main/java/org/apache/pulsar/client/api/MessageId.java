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
package org.apache.pulsar.client.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Opaque unique identifier of a single message
 *
 * <p>The MessageId can be used to reference a specific message, for example when acknowledging, without having
 * to retain the message content in memory for an extended period of time.
 *
 * <p>Message ids are {@link Comparable} and a bigger message id will imply that a message was published "after"
 * the other one.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageId extends Comparable<MessageId>, Serializable {

    /**
     * Serialize the message ID into a byte array.
     *
     * <p>The serialized message id can be stored away and later get deserialized by
     * using {@link #fromByteArray(byte[])}.
     */
    byte[] toByteArray();

    /**
     * De-serialize a message id from a byte array.
     *
     * @param data
     *            byte array containing the serialized message id
     * @return the de-serialized messageId object
     * @throws IOException if the de-serialization fails
     */
    static MessageId fromByteArray(byte[] data) throws IOException {
        return DefaultImplementation.getDefaultImplementation().newMessageIdFromByteArray(data);
    }

    /**
     * De-serialize a message id from a byte array with its topic
     * information attached.
     *
     * <p>The topic information is needed when acknowledging a {@link MessageId} on
     * a consumer that is consuming from multiple topics.
     *
     * @param data the byte array with the serialized message id
     * @param topicName the topic name
     * @return a {@link MessageId instance}
     * @throws IOException if the de-serialization fails
     */
    static MessageId fromByteArrayWithTopic(byte[] data, String topicName) throws IOException {
        return DefaultImplementation.getDefaultImplementation().newMessageIdFromByteArrayWithTopic(data, topicName);
    }

    // CHECKSTYLE.OFF: ConstantName

    /**
     * MessageId that represents the oldest message available in the topic.
     */
    MessageId earliest = DefaultImplementation.getDefaultImplementation().newMessageId(-1, -1, -1);

    /**
     * MessageId that represents the next message published in the topic.
     */
    MessageId latest = DefaultImplementation.getDefaultImplementation().newMessageId(Long.MAX_VALUE, Long.MAX_VALUE, -1);

    // CHECKSTYLE.ON: ConstantName
}
