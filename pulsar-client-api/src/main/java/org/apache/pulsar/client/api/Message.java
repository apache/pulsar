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

import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.common.api.EncryptionContext;

/**
 * The message abstraction used in Pulsar.
 */
public interface Message<T> {

    /**
     * Return the properties attached to the message.
     *
     * <p>Properties are application defined key/value pairs that will be attached to the message.
     *
     * @return an unmodifiable view of the properties map
     */
    Map<String, String> getProperties();

    /**
     * Check whether the message has a specific property attached.
     *
     * @param name the name of the property to check
     * @return true if the message has the specified property and false if the properties is not defined
     */
    boolean hasProperty(String name);

    /**
     * Get the value of a specific property.
     *
     * @param name the name of the property
     * @return the value of the property or null if the property was not defined
     */
    String getProperty(String name);

    /**
     * Get the raw payload of the message.
     *
     * <p>Even when using the Schema and type-safe API, an application
     * has access to the underlying raw message payload.
     *
     * @return the byte array with the message payload
     */
    byte[] getData();

    /**
     * Get the de-serialized value of the message, according the configured {@link Schema}.
     *
     * @return the deserialized value of the message
     */
    T getValue();

    /**
     * Get the unique message ID associated with this message.
     *
     * <p>The message id can be used to univocally refer to a message without having the keep
     * the entire payload in memory.
     *
     * <p>Only messages received from the consumer will have a message id assigned.
     *
     * @return the message id null if this message was not received by this client instance
     */
    MessageId getMessageId();

    /**
     * Get the publish time of this message. The publish time is the timestamp that a client publish the message.
     *
     * @return publish time of this message.
     * @see #getEventTime()
     */
    long getPublishTime();

    /**
     * Get the event time associated with this message. It is typically set by the applications via
     * {@link MessageBuilder#setEventTime(long)}.
     *
     * <p>If there isn't any event time associated with this event, it will return 0.
     *
     * @see MessageBuilder#setEventTime(long)
     * @since 1.20.0
     * @return the message event time or 0 if event time wasn't set
     */
    long getEventTime();

    /**
     * Get the sequence id associated with this message. It is typically set by the applications via
     * {@link MessageBuilder#setSequenceId(long)}.
     *
     * @return sequence id associated with this message.
     * @see MessageBuilder#setEventTime(long)
     * @since 1.22.0
     */
    long getSequenceId();

    /**
     * Get the producer name who produced this message.
     *
     * @return producer name who produced this message, null if producer name is not set.
     * @since 1.22.0
     */
    String getProducerName();

    /**
     * Check whether the message has a key.
     *
     * @return true if the key was set while creating the message and false if the key was not set
     * while creating the message
     */
    boolean hasKey();

    /**
     * Get the key of the message.
     *
     * @return the key of the message
     */
    String getKey();

    /**
     * Check whether the key has been base64 encoded.
     *
     * @return true if the key is base64 encoded, false otherwise
     */
    boolean hasBase64EncodedKey();

    /**
     * Get bytes in key. If the key has been base64 encoded, it is decoded before being returned.
     * Otherwise, if the key is a plain string, this method returns the UTF_8 encoded bytes of the string.
     * @return the key in byte[] form
     */
    byte[] getKeyBytes();

    /**
     * Check whether the message has a ordering key.
     *
     * @return true if the ordering key was set while creating the message
     *         false if the ordering key was not set while creating the message
     */
    boolean hasOrderingKey();

    /**
     * Get the ordering key of the message.
     *
     * @return the ordering key of the message
     */
    byte[] getOrderingKey();

    /**
     * Get the topic the message was published to.
     *
     * @return the topic the message was published to
     */
    String getTopicName();

    /**
     * {@link EncryptionContext} contains encryption and compression information in it using which application can
     * decrypt consumed message with encrypted-payload.
     *
     * @return the optiona encryption context
     */
    Optional<EncryptionContext> getEncryptionCtx();

    /**
     * Get message redelivery count, redelivery count maintain in pulsar broker. When client acknowledge message
     * timeout, broker will dispatch message again with message redelivery count in CommandMessage defined.
     *
     * <p>Message redelivery increases monotonically in a broker, when topic switch ownership to a another broker
     * redelivery count will be recalculated.
     *
     * @since 2.3.0
     * @return message redelivery count
     */
    int getRedeliveryCount();

    /**
     * Get schema version of the message.
     * @since 2.4.0
     * @return Schema version of the message if the message is produced with schema otherwise null.
     */
    byte[] getSchemaVersion();

    /**
     * Check whether the message is replicated from other cluster.
     *
     * @since 2.4.0
     * @return true if the message is replicated from other cluster.
     *         false otherwise.
     */
    boolean isReplicated();

    /**
     * Get name of cluster, from which the message is replicated.
     *
     * @since 2.4.0
     * @return the name of cluster, from which the message is replicated.
     */
    String getReplicatedFrom();
}
