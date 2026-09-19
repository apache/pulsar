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
package org.apache.pulsar.client.api.v5;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * An immutable message received from a Pulsar topic.
 *
 * @param <T> the type of the deserialized message value
 */
public interface Message<T> {

    /**
     * The deserialized value of the message according to the schema.
     *
     * @return the deserialized message value
     */
    T value();

    /**
     * The raw bytes of the message payload.
     *
     * @return the raw payload as a byte array
     */
    byte[] data();

    /**
     * The unique identifier of this message within the topic.
     *
     * @return the {@link MessageId} of this message
     */
    MessageId id();

    /**
     * The message key, used for per-key ordering.
     *
     * @return an {@link Optional} containing the message key, or empty if no key was set
     */
    Optional<String> key();

    /**
     * Application-defined properties attached to the message.
     *
     * @return an unmodifiable map of property key-value pairs
     */
    Map<String, String> properties();

    /**
     * The timestamp when the message was published by the broker.
     *
     * @return the publish timestamp as an {@link Instant}
     */
    Instant publishTime();

    /**
     * The event time set by the producer, if any.
     *
     * @return an {@link Optional} containing the event time, or empty if not set by the producer
     */
    Optional<Instant> eventTime();

    /**
     * The producer-assigned sequence ID for deduplication.
     *
     * @return the sequence ID of this message
     */
    long sequenceId();

    /**
     * The name of the producer that published this message.
     *
     * @return an {@link Optional} containing the producer name, or empty if not available
     */
    Optional<String> producerName();

    /**
     * The topic this message was published to.
     *
     * @return the fully qualified topic name
     */
    String topic();

    /**
     * The number of times the broker has redelivered this message.
     *
     * @return the redelivery count, starting at 0 for the first delivery
     */
    int redeliveryCount();

    /**
     * The uncompressed size of the message payload in bytes.
     *
     * @return the payload size in bytes
     */
    int size();

    /**
     * The cluster from which this message was replicated, if applicable.
     *
     * @return an {@link Optional} containing the source cluster name, or empty if the message
     *         is not replicated
     */
    Optional<String> replicatedFrom();
}
