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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;

/**
 * Builder for configuring and creating a {@link StreamConsumer}.
 *
 * @param <T> the type of message values the consumer will receive
 */
public interface StreamConsumerBuilder<T> {

    /**
     * Subscribe and create the stream consumer, blocking until ready.
     *
     * @return the created {@link StreamConsumer}
     * @throws PulsarClientException if the subscription fails or a connection error occurs
     */
    StreamConsumer<T> subscribe() throws PulsarClientException;

    /**
     * Subscribe and create the stream consumer asynchronously.
     *
     * @return a {@link CompletableFuture} that completes with the created {@link StreamConsumer}
     */
    CompletableFuture<StreamConsumer<T>> subscribeAsync();

    // --- Required ---
    // Either {@link #topic(String)} or {@link #namespace} must be set, not both.

    /**
     * Subscribe to a single scalable topic by name.
     *
     * @param topicName the fully-qualified topic name (e.g. {@code topic://tenant/ns/name})
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> topic(String topicName);

    /**
     * Subscribe to every scalable topic under a namespace. The matching set follows
     * live: when topics are created in or deleted from the namespace, the consumer
     * attaches / detaches automatically.
     *
     * @param namespace the namespace in {@code tenant/namespace} form
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> namespace(String namespace);

    /**
     * Subscribe to scalable topics under a namespace whose properties match every
     * key/value pair in {@code propertyFilters} (AND semantics). An empty map is
     * equivalent to {@link #namespace(String)} — every topic in the namespace.
     * The matching set follows live as topic properties change.
     *
     * @param namespace       the namespace in {@code tenant/namespace} form
     * @param propertyFilters property name/value pairs that all must match
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> namespace(String namespace, Map<String, String> propertyFilters);

    /**
     * The subscription name.
     *
     * @param subscriptionName the subscription name
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> subscriptionName(String subscriptionName);

    // --- Seek (initial position override) ---

    /**
     * Reset the subscription to a specific message ID.
     *
     * @param messageId the message ID to seek to
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> seek(MessageId messageId);

    /**
     * Reset the subscription to a specific timestamp. The subscription
     * will be positioned at the first message published at or after this timestamp.
     *
     * @param timestamp the timestamp to seek to
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> seek(Instant timestamp);

    // --- Optional ---

    /**
     * Properties to attach to the subscription.
     *
     * @param properties the subscription properties
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> subscriptionProperties(Map<String, String> properties);

    /**
     * Initial position when the subscription is first created (no existing cursor).
     *
     * @param position the initial position
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition position);

    /**
     * A custom name for this consumer instance.
     *
     * @param consumerName the consumer name
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> consumerName(String consumerName);

    /**
     * How frequently cumulative acknowledgments are flushed to the broker.
     *
     * @param delay the acknowledgment group time
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> acknowledgmentGroupTime(Duration delay);

    /**
     * Whether to read from the compacted topic (only latest value per key).
     *
     * @param readCompacted {@code true} to read from the compacted topic
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> readCompacted(boolean readCompacted);

    /**
     * Enable replication of subscription state across geo-replicated clusters.
     *
     * @param replicate {@code true} to replicate subscription state
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> replicateSubscriptionState(boolean replicate);

    // --- Encryption ---

    /**
     * Configure end-to-end message encryption for decryption.
     *
     * @param policy the encryption policy to use
     * @return this builder instance for chaining
     * @see EncryptionPolicy#forConsumer
     */
    StreamConsumerBuilder<T> encryptionPolicy(EncryptionPolicy policy);

    // --- Metadata ---

    /**
     * Add a single property to the consumer metadata.
     *
     * @param key   the property key
     * @param value the property value
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> property(String key, String value);

    /**
     * Add multiple properties to the consumer metadata.
     *
     * @param properties the properties to add
     * @return this builder instance for chaining
     */
    StreamConsumerBuilder<T> properties(Map<String, String> properties);
}
