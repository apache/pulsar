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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Common message metadata that can be set on any outgoing message.
 *
 * <p>This is the shared base for {@link MessageBuilder} (sync) and
 * {@link org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder AsyncMessageBuilder} (async).
 * The self-referential type parameter {@code BuilderT} enables fluent chaining
 * on both subtypes.
 *
 * @param <T>    the type of the message value
 * @param <BuilderT> the concrete builder type (for fluent returns)
 */
public interface MessageMetadata<T, BuilderT extends MessageMetadata<T, BuilderT>> {

    /**
     * Set the message value.
     *
     * @param value the message payload to be serialized using the producer's schema
     * @return this builder instance for chaining
     */
    BuilderT value(T value);

    /**
     * Set the message key. Messages with the same key are guaranteed to be delivered
     * in order to stream consumers. Queue consumers may use the key for routing.
     *
     * @param key the message key used for ordering and routing
     * @return this builder instance for chaining
     */
    BuilderT key(String key);

    /**
     * Associate this message with a transaction.
     *
     * @param txn the transaction to associate with this message
     * @return this builder instance for chaining
     */
    BuilderT transaction(Transaction txn);

    /**
     * Add a single property to the message.
     *
     * @param name the property key
     * @param value the property value
     * @return this builder instance for chaining
     */
    BuilderT property(String name, String value);

    /**
     * Add multiple properties to the message.
     *
     * @param properties a map of property key-value pairs to attach to the message
     * @return this builder instance for chaining
     */
    BuilderT properties(Map<String, String> properties);

    /**
     * Set the event time of the message.
     *
     * @param eventTime the application-defined event time for the message
     * @return this builder instance for chaining
     */
    BuilderT eventTime(Instant eventTime);

    /**
     * Set the sequence ID for producer deduplication.
     *
     * @param sequenceId the sequence ID to assign to the message for deduplication purposes
     * @return this builder instance for chaining
     */
    BuilderT sequenceId(long sequenceId);

    /**
     * Request delayed delivery: the message becomes visible to consumers after the given delay.
     *
     * @param delay the duration to wait before the message becomes visible to consumers
     * @return this builder instance for chaining
     */
    BuilderT deliverAfter(Duration delay);

    /**
     * Request delayed delivery: the message becomes visible to consumers at the given time.
     *
     * @param timestamp the absolute time at which the message becomes visible to consumers
     * @return this builder instance for chaining
     */
    BuilderT deliverAt(Instant timestamp);

    /**
     * Restrict geo-replication to the specified clusters only.
     *
     * @param clusters the list of cluster names to which this message should be replicated
     * @return this builder instance for chaining
     */
    BuilderT replicationClusters(List<String> clusters);
}
